from __future__ import annotations

import sys
from pathlib import Path

from minio import Minio

client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin123', secure=False)

if len(sys.argv) < 2:
    raise SystemExit('Uso: python scripts/upload_to_minio.py caminho/do/arquivo.csv')

path = Path(sys.argv[1])
client.fput_object('raw', path.name, str(path), content_type='text/csv')
print(f'Arquivo enviado para raw/{path.name}')
