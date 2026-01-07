"""
Utilities for generating synthetic data.
"""

import re
from generate_synthetic_data_patterns import CompiledPatterns
import string
from typing import List, Optional, Tuple, Union
import warnings

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()


def generate_random_string(length: int = 10, use_digits: bool = False) -> str:
    """
    Generate a random string of specified length.

    Args:
        length: Length of the string to generate
        use_digits: Whether to include digits in the string

    Returns:
        Random string
    """
    chars = string.ascii_letters
    if use_digits:
        chars += string.digits
    return "".join(np.random.choice(list(chars)) for _ in range(length))


def generate_random_email(domain: Optional[str] = None) -> str:
    """
    Generate a random email address.

    Args:
        domain: Optional domain to use for the email

    Returns:
        Random email address
    """
    if domain is None:
        return fake.email()
    username = fake.user_name()
    return f"{username}@{domain}"


def generate_random_phone(country_code: Optional[str] = None) -> str:
    """
    Generate a random phone number.

    Args:
        country_code: Optional country code (e.g., 'US', 'GB')

    Returns:
        Random phone number
    """
    if country_code:
        return fake.phone_number()
    return fake.phone_number()


def generate_random_date(
    start_date: str = "1900-01-01", end_date: str = "2023-12-31"
) -> str:
    """
    Generate a random date between start_date and end_date.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Random date as string in YYYY-MM-DD format
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    random_date = start + pd.Timedelta(
        days=np.random.randint(0, (end - start).days + 1)
    )
    return random_date.strftime("%Y-%m-%d")


def generate_random_datetime(
    start_datetime: str = "1900-01-01 00:00:00",
    end_datetime: str = "2023-12-31 23:59:59",
) -> str:
    """
    Generate a random datetime between start_datetime and end_datetime.

    Args:
        start_datetime: Start datetime in YYYY-MM-DD HH:MM:SS format
        end_datetime: End datetime in YYYY-MM-DD HH:MM:SS format

    Returns:
        Random datetime as string in YYYY-MM-DD HH:MM:SS format
    """
    start = pd.to_datetime(start_datetime)
    end = pd.to_datetime(end_datetime)
    random_datetime = start + pd.Timedelta(
        seconds=np.random.randint(0, int((end - start).total_seconds()) + 1)
    )
    return random_datetime.strftime("%Y-%m-%d %H:%M:%S")


def generate_random_time() -> str:
    """
    Generate a random time.

    Returns:
        Random time as string in HH:MM:SS format
    """
    hours = np.random.randint(0, 24)
    minutes = np.random.randint(0, 60)
    seconds = np.random.randint(0, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def generate_random_url(secure: bool = True) -> str:
    """
    Generate a random URL.

    Args:
        secure: Whether to use https (True) or http (False)

    Returns:
        Random URL
    """
    protocol = "https" if secure else "http"
    domain = fake.domain_name()
    return f"{protocol}://{domain}"


def generate_random_ipv4() -> str:
    """
    Generate a random IPv4 address.

    Returns:
        Random IPv4 address
    """
    return fake.ipv4()


def generate_random_ipv6() -> str:
    """
    Generate a random IPv6 address.

    Returns:
        Random IPv6 address
    """
    return fake.ipv6()


def generate_random_mac_address() -> str:
    """
    Generate a random MAC address.

    Returns:
        Random MAC address
    """
    return fake.mac_address()


def generate_random_uuid() -> str:
    """
    Generate a random UUID.

    Returns:
        Random UUID
    """
    return fake.uuid4()


def generate_random_ssn() -> str:
    """
    Generate a random Social Security Number (US format).

    Returns:
        Random SSN
    """
    return fake.ssn()


def generate_random_credit_card() -> str:
    """
    Generate a random credit card number.

    Returns:
        Random credit card number
    """
    return fake.credit_card_number()


def generate_random_iban() -> str:
    """
    Generate a random IBAN.

    Returns:
        Random IBAN
    """
    return fake.iban()


def infer_column_type(col) -> str:
    """
    Infer the semantic type of a column based on its name and data type.

    Args:
        col: Column metadata object with 'name' and 'data_type' attributes

    Returns:
        Inferred column type as string
    """
    col_name = col.name.lower()
    data_type = col.data_type.lower() if hasattr(col, "data_type") else ""

    # Age detection using compiled pattern
    if CompiledPatterns.AGE_PATTERN.search(col.name):
        return "age"

    # Email detection
    if "email" in col_name or "e_mail" in col_name or "mail" in col_name:
        return "email"

    # Phone detection
    if "phone" in col_name or "telephone" in col_name or "mobile" in col_name:
        return "phone"

    # Name detection
    if (
        col_name in ["name", "full_name", "fullname"]
        or "first_name" in col_name
        or "last_name" in col_name
        or "firstname" in col_name
        or "lastname" in col_name
    ):
        return "name"

    # Address detection
    if (
        "address" in col_name
        or "street" in col_name
        or "city" in col_name
        or "state" in col_name
        or "zip" in col_name
        or "postal" in col_name
        or "country" in col_name
    ):
        return "address"

    # Date/time detection
    if "date" in data_type:
        return "date"
    if "time" in data_type and "datetime" not in data_type:
        return "time"
    if "datetime" in data_type or "timestamp" in data_type:
        return "datetime"

    # Numeric detection
    if any(
        t in data_type
        for t in ["int", "integer", "bigint", "smallint", "tinyint", "numeric"]
    ):
        return "integer"
    if any(
        t in data_type
        for t in ["float", "double", "real", "decimal", "numeric", "number"]
    ):
        return "float"

    # Boolean detection
    if any(t in data_type for t in ["bool", "boolean", "bit"]):
        return "boolean"

    # Default to string
    return "string"


def parse_enum_values(col) -> Optional[List[str]]:
    """
    Parse enum values from column type definition.

    Args:
        col: Column metadata object with 'column_type' attribute

    Returns:
        List of enum values or None if not an enum type
    """
    if not hasattr(col, "column_type") or not col.column_type:
        return None

    col_type = col.column_type.lower()

    if "enum" in col_type or "set" in col_type:
        # Extract values between quotes using compiled pattern
        m = CompiledPatterns.ENUM_PATTERN.findall(col.column_type or "")
        if m:
            # Replace escaped single quotes with single quotes
            return [v.replace("''", "'") for v in m]

    if col_type.startswith("set("):
        # Extract values from SET definition using compiled pattern
        m = CompiledPatterns.ENUM_PATTERN.findall(col.column_type or "")
        if m:
            # Replace escaped single quotes with single quotes
            return [v.replace("''", "'") for v in m]

    return None


def generate_value_for_column(
    col, inferred_type: Optional[str] = None, enum_values: Optional[List[str]] = None
) -> Union[str, int, float, bool, None]:
    """
    Generate a synthetic value for a column based on its type.

    Args:
        col: Column metadata object
        inferred_type: Pre-inferred column type (optional)
        enum_values: Pre-parsed enum values (optional)

    Returns:
        Generated value appropriate for the column type
    """
    # Handle nullable columns
    if hasattr(col, "is_nullable") and col.is_nullable:
        if np.random.random() < 0.1:  # 10% chance of NULL
            return None

    # Use pre-computed values if available
    if enum_values is None:
        enum_values = parse_enum_values(col)

    if enum_values:
        return np.random.choice(enum_values)

    if inferred_type is None:
        inferred_type = infer_column_type(col)

    # Generate based on inferred type
    if inferred_type == "age":
        return np.random.randint(0, 100)
    elif inferred_type == "email":
        return generate_random_email()
    elif inferred_type == "phone":
        return generate_random_phone()
    elif inferred_type == "name":
        return fake.name()
    elif inferred_type == "address":
        return fake.address()
    elif inferred_type == "date":
        return generate_random_date()
    elif inferred_type == "time":
        return generate_random_time()
    elif inferred_type == "datetime":
        return generate_random_datetime()
    elif inferred_type == "integer":
        return np.random.randint(-1000000, 1000000)
    elif inferred_type == "float":
        return np.random.uniform(-1000000, 1000000)
    elif inferred_type == "boolean":
        return bool(np.random.choice([0, 1]))
    else:  # string
        return generate_random_string(length=np.random.randint(5, 20))


def generate_synthetic_row(columns: List) -> dict:
    """
    Generate a synthetic row of data for the given columns.

    Args:
        columns: List of column metadata objects

    Returns:
        Dictionary mapping column names to generated values
    """
    row = {}
    for col in columns:
        row[col.name] = generate_value_for_column(col)
    return row


def generate_synthetic_dataframe(
    columns: List, num_rows: int = 100
) -> pd.DataFrame:
    """
    Generate a pandas DataFrame with synthetic data.

    Args:
        columns: List of column metadata objects
        num_rows: Number of rows to generate

    Returns:
        DataFrame with synthetic data
    """
    # Pre-compute inferred types and enum values for better performance
    column_info = []
    for col in columns:
        enum_values = parse_enum_values(col)
        inferred_type = infer_column_type(col)
        column_info.append((col, inferred_type, enum_values))

    data = []
    for _ in range(num_rows):
        row = {}
        for col, inferred_type, enum_values in column_info:
            row[col.name] = generate_value_for_column(col, inferred_type, enum_values)
        data.append(row)

    return pd.DataFrame(data)


def validate_foreign_keys(
    df: pd.DataFrame, foreign_keys: List[Tuple[str, str, str]]
) -> bool:
    """
    Validate that foreign key constraints are satisfied.

    Args:
        df: DataFrame to validate
        foreign_keys: List of tuples (child_col, parent_table, parent_col)

    Returns:
        True if all foreign keys are valid, False otherwise
    """
    # This is a placeholder - actual implementation would need access to parent tables
    warnings.warn("Foreign key validation not fully implemented")
    return True


def apply_constraints(df: pd.DataFrame, constraints: dict) -> pd.DataFrame:
    """
    Apply constraints to a DataFrame.

    Args:
        df: Input DataFrame
        constraints: Dictionary of constraints to apply

    Returns:
        DataFrame with constraints applied
    """
    result_df = df.copy()

    for col_name, constraint in constraints.items():
        if col_name not in result_df.columns:
            continue

        if "min" in constraint:
            result_df[col_name] = result_df[col_name].clip(lower=constraint["min"])

        if "max" in constraint:
            result_df[col_name] = result_df[col_name].clip(upper=constraint["max"])

        if "unique" in constraint and constraint["unique"]:
            # Ensure uniqueness by adding a suffix if needed
            duplicates = result_df[col_name].duplicated()
            if duplicates.any():
                for idx in result_df[duplicates].index:
                    original_val = result_df.loc[idx, col_name]
                    suffix = 1
                    new_val = f"{original_val}_{suffix}"
                    while new_val in result_df[col_name].values:
                        suffix += 1
                        new_val = f"{original_val}_{suffix}"
                    result_df.loc[idx, col_name] = new_val

        if "not_null" in constraint and constraint["not_null"]:
            # Replace NULLs with default values
            if result_df[col_name].isna().any():
                if pd.api.types.is_numeric_dtype(result_df[col_name]):
                    result_df[col_name].fillna(0, inplace=True)
                else:
                    result_df[col_name].fillna("", inplace=True)

    return result_df


def sample_from_distribution(
    distribution: str, size: int, params: Optional[dict] = None
) -> np.ndarray:
    """
    Generate random samples from a specified distribution.

    Args:
        distribution: Name of the distribution (e.g., 'normal', 'uniform')
        size: Number of samples to generate
        params: Parameters for the distribution

    Returns:
        Array of random samples
    """
    if params is None:
        params = {}

    if distribution == "normal":
        mean = params.get("mean", 0)
        std = params.get("std", 1)
        return np.random.normal(mean, std, size)
    elif distribution == "uniform":
        low = params.get("low", 0)
        high = params.get("high", 1)
        return np.random.uniform(low, high, size)
    elif distribution == "exponential":
        scale = params.get("scale", 1)
        return np.random.exponential(scale, size)
    elif distribution == "poisson":
        lam = params.get("lambda", 1)
        return np.random.poisson(lam, size)
    elif distribution == "binomial":
        n = params.get("n", 10)
        p = params.get("p", 0.5)
        return np.random.binomial(n, p, size)
    else:
        raise ValueError(f"Unknown distribution: {distribution}")


def generate_correlated_columns(
    df: pd.DataFrame, correlations: dict
) -> pd.DataFrame:
    """
    Generate correlated columns based on existing columns.

    Args:
        df: Input DataFrame
        correlations: Dictionary specifying correlations

    Returns:
        DataFrame with correlated columns added
    """
    result_df = df.copy()

    for target_col, source_info in correlations.items():
        source_col = source_info["source"]
        correlation = source_info["correlation"]

        if source_col not in result_df.columns:
            continue

        # Generate correlated values
        source_values = result_df[source_col].values
        if pd.api.types.is_numeric_dtype(result_df[source_col]):
            noise = np.random.randn(len(source_values))
            correlated_values = correlation * source_values + (
                1 - abs(correlation)
            ) * noise
            result_df[target_col] = correlated_values
        else:
            # For non-numeric columns, just copy with some randomization
            result_df[target_col] = source_values

    return result_df


def parse_set_values(set_definition: Optional[str]) -> Optional[List[str]]:
    """
    Parse SET values from a MySQL SET column definition.

    Args:
        set_definition: SET column definition string (e.g., "SET('a','b','c')")

    Returns:
        List of SET values or None if not a valid SET definition
    """
    if not set_definition:
        return None

    set_def = set_definition.strip().lower()
    if not set_def.startswith("set("):
        return None

    # Extract values between quotes using compiled pattern
    m = CompiledPatterns.ENUM_PATTERN.findall(set_definition or "")
    if m:
        # Replace escaped single quotes with single quotes
        return [v.replace("''", "'") for v in m]

    return None


def generate_set_value(set_values: List[str]) -> str:
    """
    Generate a random SET value (comma-separated subset of allowed values).

    Args:
        set_values: List of allowed SET values

    Returns:
        Comma-separated string of randomly selected values
    """
    if not set_values:
        return ""

    # Randomly select 0 to all values
    num_values = np.random.randint(0, len(set_values) + 1)
    if num_values == 0:
        return ""

    selected = np.random.choice(set_values, size=num_values, replace=False)
    return ",".join(selected)
