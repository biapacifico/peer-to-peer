import hashlib
import os

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
        self.original_hash: None

        if self.is_seeder:
            self._initialize_seeder()

    def _initialize_seeder(self):
        file_size = os.path.getsize(self.file_path)
        self.total_blocks = (file_size + self.block_size - 1) // self.block_size
        self.blocks_owned = set(range(self.total_blocks))

        self.original_hash = calculate_sha256(self.file_path)

        print(f"[SEEDER] Arquivo {self.file_path} fragmentado em {self.total_blocks} blocos.")
        print(f"[SEEDER] Hash SHA-256 do arquivo original: {self.original_hash}")