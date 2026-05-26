import asyncio
import os
import shutil
import time
from peer import Peer

# Cria arquivo com conteúdo aleatório e retorna o caminho
def make_file(name: str, size_bytes: int) -> str:
    with open(name, "wb") as f:
        chunk = 64 * 1024
        written = 0
        while written < size_bytes:
            f.write(os.urandom(min(chunk, size_bytes - written)))
            written += min(chunk, size_bytes - written)
    return name

def cleanup(*paths):
    for p in paths:
        if os.path.isfile(p):
            os.remove(p)
        elif os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)


#  2 peers:  seeder(6001) ← leecher(6002)
#  4 peers:  seeder(6001) ← leecher_a(6002) ← leecher_b(6003) ← leecher_c(6004)
def neighbors_2():
    return {
        6001: [],
        6002: [("127.0.0.1", 6001)],
    }

def neighbors_4():
    return {
        6001: [],
        6002: [("127.0.0.1", 6001)],
        6003: [("127.0.0.1", 6001), ("127.0.0.1", 6002)],
        6004: [("127.0.0.1", 6003)],
    }


async def run_case(
    label: str,
    file_path: str,
    block_size: int,
    neighbor_map: dict,
):
    ports = sorted(neighbor_map.keys())
    seeder_p = ports[0]
    leecher_ps = ports[1:]

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Peers: {len(ports)}  |  Bloco: {block_size}B  |  "
          f"Arquivo: {os.path.basename(file_path)} ({os.path.getsize(file_path)//1024} KB)")
    print(f"{'='*60}")

    seeder = Peer("127.0.0.1", seeder_p, neighbor_map[seeder_p], is_seeder=True, file_path=file_path, block_size=block_size)

    leechers = [
        Peer("127.0.0.1", p, neighbor_map[p], block_size=block_size)
        for p in leecher_ps
    ]

    server_tasks = []
    server_tasks.append(asyncio.create_task(seeder.start_server()))
    for leecher in leechers:
        server_tasks.append(asyncio.create_task(leecher.start_server()))
    await asyncio.sleep(0.2)

    t0 = time.perf_counter()
    await asyncio.gather(*[leecher.download() for leecher in leechers])
    elapsed = time.perf_counter() - t0

    all_ok = True
    for leecher in leechers:
        output = await leecher.assemble()
        ok     = await leecher.verify(output)
        all_ok = all_ok and ok

    print(f"\nTempo total: {elapsed:.2f}s")
    print(f"  {'PASSOU' if all_ok else 'FALHOU'}")

    for task in server_tasks:
        task.cancel()
    await asyncio.gather(*server_tasks, return_exceptions=True)
    await asyncio.sleep(0.1)

    paths_to_clean = [f"received_{os.path.basename(file_path)}"]
    for p in ports:
        paths_to_clean.append(f"peer_{p}_blocks")
    cleanup(*paths_to_clean)

    return all_ok


async def main():
    results = []

    # Files de teste (10KB, 20KB, 1MB, 5MB, 10MB, 20MB)
    file_a_10 = make_file("file_a_10kb.bin", 10*1024)
    file_a_20 = make_file("file_a_20kb.bin", 20*1024)
    file_b_1 = make_file("file_b_1mb.bin", 1*1024*1024)
    file_b_5 = make_file("file_b_5mb.bin", 5*1024*1024)
    file_c_10 = make_file("file_c_10mb.bin", 10*1024*1024)
    file_c_20 = make_file("file_c_20mb.bin", 20*1024*1024)

    # Caso 1: File A 10KB, 2 peers, bloco 1KB
    results.append(await run_case(
        "Caso 1 — File A 10KB | 2 peers | bloco 1KB",
        file_a_10, 1024, neighbors_2(),
    ))

    # Caso 2: File A 20KB, 2 peers, bloco 1KB
    results.append(await run_case(
        "Caso 2 — File A 20KB | 2 peers | bloco 1KB",
        file_a_20, 1024, neighbors_2(),
    ))

    # Caso 3: File A 10KB, 2 peers, bloco 4KB
    results.append(await run_case(
        "Caso 3 — File A 10KB | 2 peers | bloco 4KB",
        file_a_10, 4096, neighbors_2(),
    ))

    # Caso 4: File A 10KB, 4 peers, bloco 1KB
    results.append(await run_case(
        "Caso 4 — File A 10KB | 4 peers | bloco 1KB",
        file_a_10, 1024, neighbors_4(),
    ))

    # Caso 5: File B 1MB, 2 peers, bloco 1KB
    results.append(await run_case(
        "Caso 5 — File B 1MB | 2 peers | bloco 1KB",
        file_b_1, 1024, neighbors_2(),
    ))

    # Caso 6: File B 5MB, 2 peers, bloco 1KB
    results.append(await run_case(
        "Caso 6 — File B 5MB | 2 peers | bloco 1KB",
        file_b_5, 1024, neighbors_2(),
    ))

    # Caso 7: File B 1MB, 4 peers, bloco 1KB
    results.append(await run_case(
        "Caso 7 — File B 1MB | 4 peers | bloco 1KB",
        file_b_1, 1024, neighbors_4(),
    ))

    # Caso 8: File C 10MB, 2 peers, bloco 1KB
    results.append(await run_case(
        "Caso 8 — File C 10MB | 2 peers | bloco 1KB",
        file_c_10, 1024, neighbors_2(),
    ))

    # Caso 9: File C 10MB, 4 peers, bloco 1KB
    results.append(await run_case(
        "Caso 9 — File C 10MB | 4 peers | bloco 1KB",
        file_c_10, 1024, neighbors_4(),
    ))

    # Caso 10: File C 10MB, 2 peers, bloco 4KB
    results.append(await run_case(
        "Caso 10 — File C 10MB | 2 peers | bloco 4KB",
        file_c_10, 4096, neighbors_2(),
    ))

    # Caso 11: File C 20MB, 2 peers, bloco 1KB
    results.append(await run_case(
        "Caso 11 — File C 20MB | 2 peers | bloco 1KB",
        file_c_20, 1024, neighbors_2(),
    ))
 
    # Caso 12: File C 20MB, 4 peers, bloco 1KB
    results.append(await run_case(
        "Caso 12 — File C 20MB | 4 peers | bloco 1KB",
        file_c_20, 1024, neighbors_4(),
    ))

    # Caso 13: File C 20MB, 2 peers, bloco 4KB
    results.append(await run_case(
        "Caso 13 — File C 20MB | 2 peers | bloco 4KB",
        file_c_20, 4096, neighbors_2(),
    ))

    cleanup(file_a_10, file_a_20, file_b_1, file_b_5, file_c_10, file_c_20)

    print(f" RESUMO: {sum(results)}/{len(results)} casos passaram")


if __name__ == "__main__":
    asyncio.run(main())