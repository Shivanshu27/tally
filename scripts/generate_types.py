"""Generate TypeScript types from FastAPI OpenAPI specification."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tally.api.app import app


def resolve_type(schema: dict[str, Any]) -> str:
    """Recursively convert OpenAPI / JSON Schema type to TypeScript type."""
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]

    if "anyOf" in schema:
        types = [resolve_type(sub) for sub in schema["anyOf"]]
        return " | ".join(types)

    if "enum" in schema:
        return " | ".join(f"'{val}'" for val in schema["enum"])

    t = schema.get("type")
    if t == "string":
        return "string"
    if t in ("integer", "number"):
        return "number"
    if t == "boolean":
        return "boolean"
    if t == "null":
        return "null"
    if t == "array":
        items = schema.get("items", {})
        item_type = resolve_type(items)
        if " | " in item_type:
            return f"({item_type})[]"
        return f"{item_type}[]"
    if t == "object":
        add_props = schema.get("additionalProperties")
        if add_props is True or add_props is None:
            return "Record<string, any>"
        if isinstance(add_props, dict):
            val_type = resolve_type(add_props)
            return f"Record<string, {val_type}>"
        return "Record<string, any>"

    return "any"


def generate_ts() -> str:
    openapi = app.openapi()
    schemas = openapi.get("components", {}).get("schemas", {})

    lines: list[str] = [
        "/**",
        " * AUTO-GENERATED FILE - DO NOT EDIT MANUALLY.",
        " * Generated from FastAPI OpenAPI 3.1 schema by scripts/generate_types.py.",
        " * Implements requirement: 'Types generated from the OpenAPI schema, not hand-written.'",
        " */",
        "",
    ]

    for name, schema in schemas.items():
        doc = schema.get("description", "")
        if doc:
            lines.append(f"/** {doc} */")

        if "enum" in schema:
            values = " | ".join(f"'{v}'" for v in schema["enum"])
            lines.append(f"export type {name} = {values};")
            lines.append("")
            continue

        if schema.get("type") == "object" or "properties" in schema:
            lines.append(f"export interface {name} {{")
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))

            for prop_name, prop_schema in properties.items():
                p_doc = prop_schema.get("description", "")
                if p_doc:
                    lines.append(f"  /** {p_doc} */")
                opt = "" if prop_name in required else "?"
                p_type = resolve_type(prop_schema)
                lines.append(f"  {prop_name}{opt}: {p_type};")

            lines.append("}")
            lines.append("")
        else:
            ts_type = resolve_type(schema)
            lines.append(f"export type {name} = {ts_type};")
            lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    out_path = Path("web/src/types.generated.ts")
    ts_code = generate_ts()
    out_path.write_text(ts_code)
    print(f"Generated {out_path} ({len(ts_code)} bytes)")
