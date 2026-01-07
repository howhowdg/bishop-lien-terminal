"""Parsing utilities for tax lien data normalization."""

import re
from typing import Optional


def parse_currency(value: Optional[str]) -> Optional[float]:
    """
    Parse currency string to float.

    Examples:
        '$1,234.56' -> 1234.56
        '1234.56' -> 1234.56
        '$1,234' -> 1234.0
        '($500.00)' -> -500.0  # Parentheses = negative in accounting

    Args:
        value: Currency string to parse

    Returns:
        Float value or None if parsing fails
    """
    if not value:
        return None

    try:
        value = str(value).strip()

        # Check for accounting negative (parentheses)
        is_negative = value.startswith("(") and value.endswith(")")
        if is_negative:
            value = value[1:-1]

        # Remove currency symbols, commas, whitespace
        cleaned = re.sub(r"[$,\s]", "", value)

        if not cleaned:
            return None

        result = float(cleaned)
        return -result if is_negative else result

    except (ValueError, TypeError):
        return None


def parse_percentage(value: Optional[str]) -> Optional[float]:
    """
    Parse percentage string to float.

    Examples:
        '18%' -> 18.0
        '18.5 %' -> 18.5
        '0.18' -> 18.0  # Assumes decimal if < 1
        '18' -> 18.0

    Args:
        value: Percentage string to parse

    Returns:
        Float percentage value or None if parsing fails
    """
    if not value:
        return None

    try:
        value = str(value).strip()

        # Remove percent sign and whitespace
        cleaned = re.sub(r"[%\s]", "", value)

        if not cleaned:
            return None

        result = float(cleaned)

        # If value is between 0 and 1, assume it's a decimal percentage
        if 0 < result < 1:
            result = result * 100

        return result

    except (ValueError, TypeError):
        return None


def clean_parcel_id(parcel: Optional[str]) -> Optional[str]:
    """
    Clean and normalize parcel ID.

    Different counties use different formats:
    - 12-34-56-78 (hyphens)
    - 12.34.56.78 (dots)
    - 1234567890 (no separators)
    - R1234567890 (with prefix)

    This preserves the original format but strips whitespace
    and normalizes case for any letter prefixes.

    Args:
        parcel: Raw parcel ID string

    Returns:
        Cleaned parcel ID or None
    """
    if not parcel:
        return None

    # Strip whitespace, normalize to uppercase
    cleaned = str(parcel).strip().upper()

    # Remove any leading/trailing quotes
    cleaned = cleaned.strip("\"'")

    return cleaned if cleaned else None


def parse_address(address: Optional[str]) -> dict:
    """
    Parse address string into components.

    Args:
        address: Full address string

    Returns:
        Dict with parsed components:
        - street_number
        - street_name
        - city
        - state
        - zip_code
    """
    if not address:
        return {}

    result = {
        "street_number": None,
        "street_name": None,
        "city": None,
        "state": None,
        "zip_code": None,
        "raw": address
    }

    try:
        address = str(address).strip()

        # Try to extract ZIP code
        zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\b", address)
        if zip_match:
            result["zip_code"] = zip_match.group(1)

        # Try to extract state (two letter code before ZIP)
        state_match = re.search(r"\b([A-Z]{2})\s+\d{5}", address.upper())
        if state_match:
            result["state"] = state_match.group(1)

        # Try to extract street number (leading digits)
        number_match = re.match(r"^(\d+)\s+", address)
        if number_match:
            result["street_number"] = number_match.group(1)

    except Exception:
        pass

    return result


def format_currency(value: Optional[float]) -> str:
    """
    Format float as currency string.

    Args:
        value: Numeric value

    Returns:
        Formatted currency string (e.g., '$1,234.56')
    """
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def format_percentage(value: Optional[float]) -> str:
    """
    Format float as percentage string.

    Args:
        value: Numeric value

    Returns:
        Formatted percentage string (e.g., '18.50%')
    """
    if value is None:
        return "N/A"
    return f"{value:.2f}%"
