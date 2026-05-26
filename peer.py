import argparse
import asyncio
import hashlib
import json
import os
import struct

MSG_REQUEST = 0x01
MSG_SEND = 0x02
MSG_NOTFOUND = 0x03
MSG_META_REQUEST = 0x04
MSG_META_RESPONSE = 0x05

def calculate_sha256(file_path):
    sha256_hash = hashlib.sha256()

    try:
        with open(file_path, "rb") as file:
            while True:
                byte_block = file.read(4096)
                if byte_block:
                    sha256_hash.update(byte_block)
                else:
                    break
        return sha256_hash.hexdigest()

    except FileNotFoundError:
        return None

class Peer:
    def __init__(self, host, port, neighbors, is_seeder=False, file_path=None, block_size=1024):
        self.host = host
        self.port = port
        self.neighbors = neighbors

        self.is_seeder = is_seeder
        self.file_path = file_path
        self.block_size = block_size

        self.blocks_owned = set()
        self.total_blocks = 0
        self.original_hash = None
        self.file_name = None

        self._storage_dir = f"peer_{self.port}_blocks"
        os.makedirs(self._storage_dir, exist_ok=True)

        if self.is_seeder:
            self._initialize_seeder()

    def _initialize_seeder(self):
        file_size = os.path.getsize(self.file_path)
        self.total_blocks = (file_size + self.block_size - 1) // self.block_size
        self.blocks_owned = set(range(self.total_blocks))
        self.original_hash = calculate_sha256(self.file_path)
        self.file_name = os.path.basename(self.file_path)

        print(f"[SEEDER] Arquivo {self.file_path} fragmentado em {self.total_blocks} blocos.")
        print(f"[SEEDER] Hash SHA-256 do arquivo original: {self.original_hash}")

    # Metadados
    def _build_metada(self):
        meta = {
            "file_name":    self.file_name,
            "total_blocks": self.total_blocks,
            "block_size":   self.block_size,
            "sha256":       self.original_hash,
            "blocks_owned": sorted(self.blocks_owned),
        }
        return json.dumps(meta).encode()
 
    def _apply_metada(self, raw):
        meta = json.loads(raw.decode())
        self.file_name = meta["file_name"]
        self.total_blocks = meta["total_blocks"]
        self.block_size = meta["block_size"]
        self.original_hash = meta["sha256"]

        print(f"[LEECHER {self.port}] Metadados recebidos — '{self.file_name}'" f"{self.total_blocks} blocos × {self.block_size}B")
        print(f"[LEECHER {self.port}] SHA-256 esperado: {self.original_hash}")

    # Disco
    async def _read_block(self, index):
        def _read():
            if self.is_seeder and self.file_path:
                with open(self.file_path, "rb") as f:
                    f.seek(index * self.block_size)
                    return f.read(self.block_size)

            path = os.path.join(self._storage_dir, f"block_{index}.bin")
            try:
                with open(path, "rb") as f:
                    return f.read()
            except FileNotFoundError:
                return None

        return await asyncio.to_thread(_read)

    async def save_block(self, index, data):
        def _write():
            path = os.path.join(self._storage_dir, f"block_{index}.bin")
            with open(path, "wb") as f:
                f.write(data)
 
        await asyncio.to_thread(_write)
        self.blocks_owned.add(index)

        print(f"[PEER {self.port}] Bloco {index} salvo")

    # Remontagem e verificação
    async def assemble(self, output_dir: str = "."):
        if len(self.blocks_owned) < self.total_blocks:
            missing = self.total_blocks - len(self.blocks_owned)
            raise RuntimeError(f"Download incompleto — faltam {missing} blocos")
 
        output_path = os.path.join(output_dir, f"received_{self.file_name}")
 
        def _assemble():
            with open(output_path, "wb") as out:
                for i in range(self.total_blocks):
                    block_path = os.path.join(self._storage_dir, f"block_{i}.bin")
                    with open(block_path, "rb") as bf:
                        out.write(bf.read())
 
        await asyncio.to_thread(_assemble)
        print(f"[PEER {self.port}] Arquivo remontado → '{output_path}'")
        return output_path

    async def verify(self, output_path: str):
        received_hash = await asyncio.to_thread(calculate_sha256, output_path)
        file_size = os.path.getsize(output_path)

        print(f"\n[PEER {self.port}] Verificação de integridade")
        print(f"[PEER {self.port}] Arquivo: {output_path}")
        print(f"[PEER {self.port}] Tamanho: {file_size} bytes")
        print(f"[PEER {self.port}] SHA-256 recebido: {received_hash}")
        print(f"[PEER {self.port}] SHA-256 esperado: {self.original_hash}")

        ok = received_hash == self.original_hash
        print(f"[PEER {self.port}] Integridade: {'OK' if ok else 'CORROMPIDO'}")
        return ok
 
    # Servidor
    async def start_server(self):
        server = await asyncio.start_server(self._handle_client, self.host, self.port)
        print(f"[SERVER {self.port}] Ouvindo em {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        peer_id = f"{addr[0]}:{addr[1]}"
        print(f"[SERVER {self.port}] Conexão de {peer_id}")
 
        try:
            while True:
                msg_type_bytes = await reader.readexactly(1)
                (msg_type,) = struct.unpack("!B", msg_type_bytes)
 
                if msg_type == MSG_META_REQUEST:
                    raw = self._build_metada()
                    writer.write(struct.pack("!BI", MSG_META_RESPONSE, len(raw)) + raw)
                    await writer.drain()
                    print(f"[SERVER {self.port}] Metadados enviados para {peer_id}")
                    continue
 
                if msg_type == MSG_REQUEST:
                    index_bytes = await reader.readexactly(4)
                    (block_index,) = struct.unpack("!I", index_bytes)
 
                    if block_index not in self.blocks_owned:
                        writer.write(struct.pack("!BI", MSG_NOTFOUND, block_index))
                        await writer.drain()
                        continue
 
                    data = await self._read_block(block_index)
                    if data is None:
                        writer.write(struct.pack("!BI", MSG_NOTFOUND, block_index))
                        await writer.drain()
                        continue
 
                    writer.write(struct.pack("!BII", MSG_SEND, block_index, len(data)) + data)
                    await writer.drain()
                    print(f"[SERVER {self.port}] Bloco {block_index} ({len(data)}B) → {peer_id}")
                    continue
 
                print(f"[SERVER {self.port}] Tipo desconhecido: {msg_type:#04x}")
                break
 
        except asyncio.IncompleteReadError:
            print(f"[SERVER {self.port}] {peer_id} desconectou")
        finally:
            writer.close()
            await writer.wait_closed()

    # Leecher
    async def download(self):
        await self._fetch_meta()
        
        missing = [i for i in range(self.total_blocks) if i not in self.blocks_owned]
        print(f"[LEECHER {self.port}] Iniciando download — {len(missing)} blocos faltando")
        
        tasks = [self._download_from(host, port) for host, port in self.neighbors]
        await asyncio.gather(*tasks)
        print(f"[LEECHER {self.port}] Download concluído — "
              f"{len(self.blocks_owned)}/{self.total_blocks} blocos")
 
    async def _fetch_meta(self):
        for attempt in range(20):
            for host, port in self.neighbors:
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                except OSError:
                    continue
                try:
                    writer.write(struct.pack("!B", MSG_META_REQUEST))
                    await writer.drain()
                    header = await reader.readexactly(5)
                    msg_type, size = struct.unpack("!BI", header)
                    if msg_type == MSG_META_RESPONSE:
                        raw = await reader.readexactly(size)
                        meta = json.loads(raw.decode())
                        if meta["total_blocks"] > 0:
                            self._apply_metada(raw)
                            return
                except asyncio.IncompleteReadError:
                    pass
                finally:
                    writer.close()
                    await writer.wait_closed()
            print(f"[LEECHER {self.port}] Metadados indisponíveis (tentativa {attempt+1})")
            await asyncio.sleep(0.5)
        raise RuntimeError(f"[LEECHER {self.port}] Nenhum vizinho forneceu metadados após retries")
 
    async def _download_from(self, host, port):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            print(f"[LEECHER {self.port}] Conectado a {host}:{port}")
        except OSError as e:
            print(f"[LEECHER {self.port}] Não conseguiu conectar a {host}:{port} — {e}")
            return
        try:
            while True:
                missing = [i for i in range(self.total_blocks) if i not in self.blocks_owned]
                if not missing:
                    break
                block_index = missing[0]
                writer.write(struct.pack("!BI", MSG_REQUEST, block_index))
                await writer.drain()
                header = await reader.readexactly(5)
                msg_type, recv_index = struct.unpack("!BI", header)
                if msg_type == MSG_NOTFOUND:
                    print(f"[LEECHER {self.port}] Vizinho {port} não tem bloco {block_index}")
                    continue
                if msg_type == MSG_SEND:
                    size_bytes = await reader.readexactly(4)
                    (size,) = struct.unpack("!I", size_bytes)
                    data = await reader.readexactly(size)
                    await self.save_block(recv_index, data)
        except asyncio.IncompleteReadError:
            print(f"[LEECHER {self.port}] Vizinho {port} fechou conexão inesperadamente")
        finally:
            writer.close()
            await writer.wait_closed()