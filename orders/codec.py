"""Both topics use Avro binary values and version-controlled writer schemas."""

import json
from importlib.resources import files
from io import BytesIO

from fastavro import parse_schema, schemaless_reader, schemaless_writer


def load_schema(name):
    return parse_schema(json.loads(files("orders").joinpath(f"schemas/{name}.avsc").read_text()))


ORDER_SCHEMA = load_schema("order")
DLQ_SCHEMA = load_schema("dead_letter")


def encode(record, schema=ORDER_SCHEMA):
    stream = BytesIO()
    schemaless_writer(stream, schema, record, strict=True)
    return stream.getvalue()


def decode(payload, schema=ORDER_SCHEMA):
    if payload is None:
        raise ValueError("Order value must not be null")
    stream = BytesIO(payload)
    record = schemaless_reader(stream, schema)
    # Reject a valid prefix with trailing garbage instead of silently accepting it.
    if stream.read(1):
        raise ValueError("Unexpected bytes after Avro record")
    return record
