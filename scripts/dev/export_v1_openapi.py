import argparse
from pathlib import Path

from limnopulse_api.api.openapi_contract import render_v1_openapi_contract
from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("tests/contracts/openapi/v1.json")
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render_v1_openapi_contract(
        create_app(Settings(app_env="test", auth_mode="dev"))
    )
    if args.check:
        return int(
            not args.output.exists()
            or args.output.read_text(encoding="utf-8") != rendered
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
