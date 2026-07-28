"""
Script de Automação de Upload para o Databricks Unity Catalog Volume.
Utiliza o Databricks SDK para Python (WorkspaceClient) para transferir os arquivos
JSON gerados localmente diretamente para o Volume de destino no Lakehouse.

Requisitos:
- databricks-sdk instalado (pip install databricks-sdk)
- Variáveis de ambiente configuradas (DATABRICKS_HOST e DATABRICKS_TOKEN) ou autenticação via Databricks CLI.
"""

import os
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient

# Configuração do caminho de destino no Unity Catalog Volume
# Formato padrão: /Volumes/<catalog>/<schema>/<volume_name>/
VOLUME_TARGET_PATH = os.getenv("DATABRICKS_VOLUME_PATH", "/Volumes/main/default/raw_bcb")

# Arquivos a serem transferidos
LOCAL_FILES = ["selic.json", "ipca.json"]

def upload_files_to_volume(extract_dir: Path, target_volume: str) -> None:
    """
    Realiza o upload dos arquivos JSON da pasta de extração para o Volume no Databricks.
    """
    print("Initializing Databricks WorkspaceClient...")
    try:
        w = WorkspaceClient()
    except Exception as e:
        print(f"FAILED to initialize Databricks SDK client: {e}")
        print("Certifique-se de configurar DATABRICKS_HOST e DATABRICKS_TOKEN ou executar `databricks auth login`.")
        sys.exit(1)

    for filename in LOCAL_FILES:
        local_file_path = extract_dir / filename
        if not local_file_path.exists():
            print(f"ERROR: Local file not found: {local_file_path}")
            sys.exit(1)

        remote_file_path = f"{target_volume.rstrip('/')}/{filename}"
        print(f"Uploading '{local_file_path.name}' to Unity Catalog Volume: '{remote_file_path}'...")

        try:
            with open(local_file_path, "rb") as f:
                w.files.upload(
                    file_path=remote_file_path,
                    contents=f,
                    overwrite=True
                )
            print(f"SUCCESS: Upload of '{filename}' completed successfully.")
        except Exception as err:
            print(f"FAILED to upload '{filename}': {err}")
            sys.exit(1)

    print("All files successfully uploaded to Unity Catalog Volume!")

if __name__ == "__main__":
    current_dir = Path(__file__).resolve().parent
    upload_files_to_volume(current_dir, VOLUME_TARGET_PATH)
