import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts" / "events"


class SchemaInvalid(Exception):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


@lru_cache(maxsize=None)
def _validator(schema_name: str) -> Draft202012Validator:
    schema = json.loads((CONTRACTS_DIR / f"{schema_name}.schema.json").read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def validate_payload(schema_name: str, payload: dict) -> None:
    errors = [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}"
              for e in _validator(schema_name).iter_errors(payload)]
    if errors:
        raise SchemaInvalid(errors)
