"""
Shared utility functions for CSM Dashboard
"""

import pandas as pd


def to_csv_utf8_bom(df: pd.DataFrame) -> bytes:
    """
    Convert DataFrame to CSV with UTF-8 BOM encoding.

    BOM (Byte Order Mark) ensures proper display of Spanish characters
    in Excel and other applications.

    Args:
        df: DataFrame to convert

    Returns:
        CSV bytes with UTF-8 BOM encoding
    """
    csv_string = df.to_csv(index=False, encoding='utf-8')
    csv_bytes = '\ufeff' + csv_string
    return csv_bytes.encode('utf-8')
