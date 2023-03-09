from argparse import ArgumentParser
import json
from pathlib import Path

from fastapi.openapi.utils import get_openapi

from .main import app

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('out_path', type=Path, help='Path for OpenAPI JSON output')

    args = parser.parse_args()
    out_path = args.out_path

    with out_path.open('w') as f:
        json.dump(app.openapi(), f)