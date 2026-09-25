from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl


def needs_text_cast(dtype: pl.DataType) -> bool:
    return dtype.is_integer() or isinstance(dtype, (pl.List, pl.Array, pl.Struct, pl.Object))


def prepare(source: Path, destination: Path) -> None:
    frame = pl.read_parquet(source)
    text_columns = []
    expressions = []
    for name, dtype in frame.schema.items():
        if not needs_text_cast(dtype):
            continue
        text_columns.append(name)
        if dtype.is_integer():
            expressions.append(pl.col(name).cast(pl.String).alias(name))
        else:
            expressions.append(
                pl.col(name).map_elements(
                    lambda value: json.dumps(value, ensure_ascii=False, default=str),
                    return_dtype=pl.String,
                ).alias(name)
            )
    if expressions:
        frame = frame.with_columns(expressions)
    frame.write_parquet(destination, compression="zstd")
    print(f"Wrote {destination} ({frame.height:,} rows, {frame.width:,} columns)")
    print(f"Text columns ({len(text_columns)}): {', '.join(text_columns) or 'none'}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert integer and nested Parquet columns to browser-safe text."
    )
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    prepare(args.source, args.destination)


if __name__ == "__main__":
    main()
