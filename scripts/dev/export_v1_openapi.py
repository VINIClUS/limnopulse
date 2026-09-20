import argparse
import os
from pathlib import Path

from limnopulse_api.api.openapi_contract import render_v1_openapi_contract
from limnopulse_api.core.config import Settings
from limnopulse_api.main import create_app

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPOSITORY_ROOT / "tests/contracts/openapi/v1.json"


def _repository_output_path(value: str) -> Path:
    candidate = Path(value)
    try:
        absolute = candidate if candidate.is_absolute() else Path.cwd() / candidate
        lexical = Path(os.path.abspath(absolute))
        resolved = absolute.resolve()
        resolved.relative_to(REPOSITORY_ROOT)
    except (OSError, RuntimeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            "--output must resolve to a path inside the repository"
        ) from error
    if resolved != lexical:
        raise argparse.ArgumentTypeError(
            "--output must not traverse symbolic links"
        )
    return resolved


def _output_option(value: str) -> Path:
    return _repository_output_path(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=_output_option, default=None, metavar="PATH"
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    output = args.output
    if output is None:
        try:
            output = _repository_output_path(str(DEFAULT_OUTPUT))
        except argparse.ArgumentTypeError as error:
            parser.error(str(error))
    rendered = render_v1_openapi_contract(
        create_app(Settings(app_env="test", auth_mode="dev"))
    )
    if args.check:
        return int(
            not output.exists() or output.read_text(encoding="utf-8") != rendered
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
