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
        json.dump(get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
            openapi_prefix=app.openapi_prefix,
        ), f)