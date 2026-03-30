from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from load.utils import run_azure_staging_ingestion


def main() -> None:
    run_azure_staging_ingestion()


if __name__ == "__main__":
    main()
