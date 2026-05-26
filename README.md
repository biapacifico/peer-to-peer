# P2P File Transfer

Transferência de arquivos peer-to-peer implementada em Python com `asyncio`. Cada peer atua simultaneamente como servidor e cliente — baixando blocos enquanto já serve os que recebeu.

## Requisitos

Python 3.12+ (apenas stdlib, sem dependências externas)

## Como executar os testes

```bash
python tests.py
```

Roda automaticamente 13 estudos de caso, cobrindo as combinações de tamanho de arquivo (10KB, 20KB, 1MB, 5MB, 10MB, 20MB), tamanho de bloco (1KB e 4KB) e quantidade de peers (2 e 4).

Ao final exibe um resumo com quantos casos passaram e o tempo de cada um.

## Estrutura

```
peer.py               # implementação principal
tests.py              # estudos de caso automatizados
peer_<porta>_blocks/  # blocos temporários de cada peer (criado automaticamente)
```