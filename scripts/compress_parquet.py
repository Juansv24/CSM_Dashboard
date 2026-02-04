"""
Parquet Compression Script for CSM Dashboard

This script compresses the original parquet file (~1.37GB) to a smaller size (~800MB)
using the following optimizations:

1. Convert text columns to category (recommendation_text, topic, subtopic)
2. Encode binary flags as int8 (tipo_territorio, predicted_class, PDET)
3. Downcast floats where safe (IPM_2018, MDM_2022 → float32)
4. Use zstd compression at level 15

The compressed file is suitable for Render.com deployment with Git LFS.
"""

import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path


# Encoding mappings for categorical columns
ENCODING_MAPPINGS = {
    "tipo_territorio": {"Municipio": 1, "Departamento": 0},
    "predicted_class": {"Incluida": 1, "Excluida": 0},
    "PDET": {1: 1, 0: 0, True: 1, False: 0},  # Already numeric but ensure int8
    "Grupo_MDM": {"C": 0, "G1": 1, "G2": 2, "G3": 3, "G4": 4, "G5": 5},
    "Cat_IICA": {"Muy Alto": 4, "Alto": 3, "Medio": 2, "Medio Bajo": 1, "Bajo": 0},
}

# Reverse mappings for decoding (used at query time)
DECODING_MAPPINGS = {
    col: {v: k for k, v in mapping.items()}
    for col, mapping in ENCODING_MAPPINGS.items()
}


def get_file_size_mb(path: str) -> float:
    """Get file size in MB."""
    return os.path.getsize(path) / (1024 * 1024)


def compress_parquet(
    input_path: str,
    output_path: str,
    mappings_output_path: str = None,
    compression: str = "zstd",
    compression_level: int = 15,
) -> dict:
    """
    Compress parquet file with optimized dtypes and encoding.

    Args:
        input_path: Path to original parquet file
        output_path: Path for compressed output
        mappings_output_path: Path to save encoding mappings JSON
        compression: Compression algorithm (zstd recommended)
        compression_level: Compression level (1-22 for zstd, 15 is good balance)

    Returns:
        dict with compression statistics
    """
    print(f"Reading input file: {input_path}")
    print(f"Input file size: {get_file_size_mb(input_path):.2f} MB")

    # Read parquet file
    df = pd.read_parquet(input_path)
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(f"Memory usage before optimization: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # Store original dtypes for reference
    original_dtypes = df.dtypes.to_dict()

    # --- Optimization 1: Encode categorical columns ---
    print("\nEncoding categorical columns...")

    for col, mapping in ENCODING_MAPPINGS.items():
        if col in df.columns:
            print(f"  - Encoding {col}: {df[col].dtype} -> int8")
            # Handle NaN values
            df[col] = df[col].map(mapping).fillna(-1).astype('int8')

    # --- Optimization 2: Convert text columns to category ---
    print("\nConverting text columns to category...")

    category_columns = [
        'recommendation_text', 'recommendation_code',
        'topic', 'subtopic',
        'dpto', 'mpio',
        'dpto_cdpmp', 'mpio_cdpmp'
    ]

    for col in category_columns:
        if col in df.columns and df[col].dtype == 'object':
            n_unique = df[col].nunique()
            print(f"  - {col}: {n_unique:,} unique values -> category")
            df[col] = df[col].astype('category')

    # --- Optimization 3: Downcast numeric columns ---
    print("\nDowncasting numeric columns...")

    # Float columns that can be float32 (sufficient precision for scores/indices)
    float32_columns = [
        'sentence_similarity', 'prediction_confidence',
        'IPM_2018', 'MDM_2022'
    ]

    for col in float32_columns:
        if col in df.columns and df[col].dtype == 'float64':
            print(f"  - {col}: float64 -> float32")
            df[col] = df[col].astype('float32')

    # Integer columns that can be int16/int32
    int_columns = ['recommendation_priority', 'recommendation_number']
    for col in int_columns:
        if col in df.columns:
            if df[col].max() < 32767 and df[col].min() > -32768:
                print(f"  - {col}: {df[col].dtype} -> int16")
                df[col] = df[col].astype('int16')

    print(f"\nMemory usage after optimization: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # --- Write compressed parquet ---
    print(f"\nWriting compressed parquet to: {output_path}")
    print(f"Compression: {compression} (level {compression_level})")

    # Create output directory if needed
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Write with compression
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression=compression,
        compression_level=compression_level,
        index=False
    )

    output_size = get_file_size_mb(output_path)
    input_size = get_file_size_mb(input_path)
    compression_ratio = (1 - output_size / input_size) * 100

    print(f"\nOutput file size: {output_size:.2f} MB")
    print(f"Compression ratio: {compression_ratio:.1f}% reduction")

    # --- Save encoding mappings ---
    if mappings_output_path:
        print(f"\nSaving encoding mappings to: {mappings_output_path}")

        mappings_data = {
            "encoding": {k: {str(kk): vv for kk, vv in v.items()}
                        for k, v in ENCODING_MAPPINGS.items()},
            "decoding": {k: {str(kk): vv for kk, vv in v.items()}
                        for k, v in DECODING_MAPPINGS.items()},
            "category_columns": category_columns,
            "original_dtypes": {k: str(v) for k, v in original_dtypes.items()},
            "compression": {
                "algorithm": compression,
                "level": compression_level,
                "input_size_mb": input_size,
                "output_size_mb": output_size,
                "reduction_percent": compression_ratio
            }
        }

        with open(mappings_output_path, 'w') as f:
            json.dump(mappings_data, f, indent=2)

    # --- Validate output ---
    print("\nValidating compressed file...")
    df_test = pd.read_parquet(output_path)
    assert len(df_test) == len(df), "Row count mismatch!"
    assert list(df_test.columns) == list(df.columns), "Column mismatch!"
    print(f"Validation passed: {len(df_test):,} rows, {len(df_test.columns)} columns")

    return {
        "input_path": input_path,
        "output_path": output_path,
        "input_size_mb": input_size,
        "output_size_mb": output_size,
        "compression_ratio": compression_ratio,
        "row_count": len(df),
        "column_count": len(df.columns)
    }


def main():
    """Main entry point for compression script."""
    # Define paths
    project_root = Path(__file__).parent.parent

    input_path = project_root / "Data" / "Data Final Dashboard.parquet"
    output_dir = project_root / "scripts" / "output"
    output_path = output_dir / "CSM_Dashboard_compressed.parquet"
    mappings_path = output_dir / "encoding_mappings.json"

    # Validate input exists
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        print("Please ensure the parquet file exists in the Data folder.")
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run compression
    print("=" * 60)
    print("CSM Dashboard Parquet Compression")
    print("=" * 60)

    result = compress_parquet(
        input_path=str(input_path),
        output_path=str(output_path),
        mappings_output_path=str(mappings_path),
        compression="zstd",
        compression_level=15
    )

    print("\n" + "=" * 60)
    print("Compression Complete!")
    print("=" * 60)
    print(f"Input:  {result['input_size_mb']:.2f} MB")
    print(f"Output: {result['output_size_mb']:.2f} MB")
    print(f"Saved:  {result['compression_ratio']:.1f}%")
    print(f"\nOutput file: {output_path}")
    print(f"Mappings file: {mappings_path}")

    print("\nNext steps:")
    print("1. Copy compressed parquet to render worktree:")
    print(f"   cp '{output_path}' '.worktrees/render-disk-duckdb/Data/'")
    print("2. Commit with Git LFS")


if __name__ == "__main__":
    main()
