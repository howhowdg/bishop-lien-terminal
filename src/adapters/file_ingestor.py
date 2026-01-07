"""File upload adapter with fuzzy column matching for manual data ingestion."""

import io
import re
from datetime import date
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from thefuzz import fuzz, process

from .base import FileSource
from ..models import TaxLien, LienBatch, SourcePlatform


class FileIngestorAdapter(FileSource):
    """
    Adapter for ingesting tax lien data from CSV/Excel files.

    Critical for states like Illinois (Cook County) where data is
    distributed as downloadable spreadsheets rather than scraped.

    Uses fuzzy string matching to auto-detect column mappings.
    """

    platform = SourcePlatform.MANUAL_UPLOAD
    supported_states = ["IL", "FL", "AZ", "NJ", "IN", "CO", "IA", "MS", "AL", "SC"]

    # Fuzzy matching candidates for each target field
    # Multiple variations to handle different county naming conventions
    COLUMN_CANDIDATES = {
        "parcel_id": [
            "parcel id",
            "parcel number",
            "parcel",
            "parcel #",
            "pin",
            "property index number",
            "apn",
            "assessor parcel number",
            "account number",
            "account",
            "tax id",
            "property id",
            "certificate number",
            "cert #",
            "key number",
        ],
        "address": [
            "property address",
            "address",
            "situs address",
            "location",
            "property location",
            "street address",
            "street",
            "site address",
        ],
        "assessed_value": [
            "assessed value",
            "assessed",
            "total assessed value",
            "just value",
            "market value",
            "property value",
            "taxable value",
            "total value",
            "fair market value",
            "fmv",
        ],
        "face_amount": [
            "face amount",
            "face value",
            "tax amount",
            "amount due",
            "total due",
            "total tax",
            "taxes due",
            "delinquent amount",
            "minimum bid",
            "opening bid",
            "upset price",
            "redemption amount",
            "lien amount",
        ],
        "interest_rate_bid": [
            "interest rate",
            "rate",
            "bid rate",
            "winning rate",
            "bid %",
            "interest %",
        ],
        "auction_date": [
            "auction date",
            "sale date",
            "tax sale date",
            "date",
        ],
        "county": [
            "county",
            "county name",
        ],
    }

    # Minimum fuzzy match score to consider a match
    FUZZY_THRESHOLD = 70

    def __init__(
        self,
        state: str,
        county: Optional[str] = None,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        column_overrides: Optional[dict] = None
    ):
        """
        Initialize file ingestor.

        Args:
            state: Two-character state code
            county: County name
            file_path: Path to local file
            file_content: Raw file bytes (for uploads)
            column_overrides: Manual column mapping overrides
                              e.g., {"PIN": "parcel_id", "TAX_AMT": "face_amount"}
        """
        super().__init__(state, county, file_path, file_content)
        self.column_overrides = column_overrides or {}
        self._detected_mappings: dict = {}

    async def fetch(self, **kwargs) -> LienBatch:
        """
        Load and normalize data from uploaded file.

        Returns:
            LienBatch with normalized TaxLien records
        """
        # Load the file into a DataFrame
        df = self._load_dataframe()

        # Detect column mappings
        self._detected_mappings = self._detect_column_mappings(df.columns.tolist())

        # Apply manual overrides
        for source_col, target_field in self.column_overrides.items():
            if source_col in df.columns:
                self._detected_mappings[source_col] = target_field

        # Transform to TaxLien records
        liens = self._transform_dataframe(df)

        return LienBatch(
            liens=liens,
            source_url=self.file_path,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=self.county
        )

    def _load_dataframe(self) -> pd.DataFrame:
        """Load file into pandas DataFrame."""
        if self.file_content:
            # From uploaded bytes
            buffer = io.BytesIO(self.file_content)
            # Detect format from magic bytes or try both
            try:
                return pd.read_excel(buffer)
            except Exception:
                buffer.seek(0)
                return pd.read_csv(buffer)

        elif self.file_path:
            # From file path
            path = Path(self.file_path)
            if path.suffix.lower() in [".xlsx", ".xls"]:
                return pd.read_excel(path)
            else:
                # Try common CSV encodings
                for encoding in ["utf-8", "latin-1", "cp1252"]:
                    try:
                        return pd.read_csv(path, encoding=encoding)
                    except UnicodeDecodeError:
                        continue
                raise ValueError(f"Could not decode file: {path}")

        raise ValueError("No file path or content provided")

    def _detect_column_mappings(self, columns: list[str]) -> dict[str, str]:
        """
        Use fuzzy matching to detect column mappings.

        Args:
            columns: List of column names from the file

        Returns:
            Dict mapping source column names to target field names
        """
        mappings = {}
        used_columns = set()

        # For each target field, find the best matching source column
        for target_field, candidates in self.COLUMN_CANDIDATES.items():
            best_match = None
            best_score = 0

            for col in columns:
                if col in used_columns:
                    continue

                col_lower = col.lower().strip()

                # Try exact match first
                if col_lower in [c.lower() for c in candidates]:
                    best_match = col
                    best_score = 100
                    break

                # Try fuzzy matching
                result = process.extractOne(
                    col_lower,
                    candidates,
                    scorer=fuzz.ratio
                )
                if result and result[1] > best_score:
                    best_score = result[1]
                    best_match = col

            if best_match and best_score >= self.FUZZY_THRESHOLD:
                mappings[best_match] = target_field
                used_columns.add(best_match)

        return mappings

    def get_detected_mappings(self) -> dict[str, str]:
        """Return the detected column mappings for UI display."""
        return self._detected_mappings.copy()

    def get_unmapped_columns(self, columns: list[str]) -> list[str]:
        """Return columns that couldn't be auto-mapped."""
        mapped = set(self._detected_mappings.keys())
        return [c for c in columns if c not in mapped]

    def _transform_dataframe(self, df: pd.DataFrame) -> list[TaxLien]:
        """Transform DataFrame rows into TaxLien records."""
        liens = []
        reverse_map = {v: k for k, v in self._detected_mappings.items()}

        for _, row in df.iterrows():
            try:
                raw_data = row.to_dict()

                # Extract and clean values
                parcel_id = self._get_mapped_value(row, reverse_map, "parcel_id")
                if not parcel_id:
                    continue  # Skip rows without parcel ID

                lien = TaxLien(
                    state=self.state,
                    county=self._get_mapped_value(row, reverse_map, "county") or self.county or "Unknown",
                    parcel_id=str(parcel_id).strip(),
                    address=self._get_mapped_value(row, reverse_map, "address"),
                    assessed_value=self._parse_numeric(
                        self._get_mapped_value(row, reverse_map, "assessed_value")
                    ),
                    face_amount=self._parse_numeric(
                        self._get_mapped_value(row, reverse_map, "face_amount")
                    ) or 0.0,
                    interest_rate_bid=self._parse_numeric(
                        self._get_mapped_value(row, reverse_map, "interest_rate_bid")
                    ),
                    auction_date=self._parse_date(
                        self._get_mapped_value(row, reverse_map, "auction_date")
                    ),
                    source_platform=self.platform,
                    raw_data=raw_data
                )
                liens.append(lien)

            except Exception:
                continue  # Skip malformed rows

        return liens

    def _get_mapped_value(
        self,
        row: pd.Series,
        reverse_map: dict,
        target_field: str
    ) -> Optional[str]:
        """Get value from row using the detected mapping."""
        source_col = reverse_map.get(target_field)
        if source_col and source_col in row.index:
            val = row[source_col]
            if pd.notna(val):
                return str(val)
        return None

    @staticmethod
    def _parse_numeric(value: Optional[str]) -> Optional[float]:
        """Parse numeric value, stripping currency symbols."""
        if not value:
            return None
        try:
            # Remove currency symbols, commas, whitespace, percent signs
            cleaned = re.sub(r"[$,%\s]", "", str(value))
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        """Parse date from various formats."""
        if not value:
            return None
        try:
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.notna(parsed):
                return parsed.date()
            return None
        except Exception:
            return None


class ColumnMappingHelper:
    """
    Helper class for interactive column mapping in the UI.

    Provides utilities for the Streamlit frontend to show
    detected mappings and allow user corrections.
    """

    @staticmethod
    def preview_file(
        file_content: bytes,
        n_rows: int = 5
    ) -> tuple[list[str], list[dict]]:
        """
        Preview file contents for mapping UI.

        Args:
            file_content: Raw file bytes
            n_rows: Number of rows to preview

        Returns:
            Tuple of (column names, preview rows as dicts)
        """
        buffer = io.BytesIO(file_content)
        try:
            df = pd.read_excel(buffer, nrows=n_rows)
        except Exception:
            buffer.seek(0)
            df = pd.read_csv(buffer, nrows=n_rows)

        columns = df.columns.tolist()
        preview = df.head(n_rows).to_dict(orient="records")
        return columns, preview

    @staticmethod
    def suggest_mappings(columns: list[str]) -> dict[str, dict]:
        """
        Suggest mappings for UI display with confidence scores.

        Returns:
            Dict mapping source columns to {target_field, confidence}
        """
        suggestions = {}
        used_targets = set()

        for col in columns:
            col_lower = col.lower().strip()
            best_match = None
            best_score = 0
            best_target = None

            for target_field, candidates in FileIngestorAdapter.COLUMN_CANDIDATES.items():
                if target_field in used_targets:
                    continue

                # Exact match
                if col_lower in [c.lower() for c in candidates]:
                    best_target = target_field
                    best_score = 100
                    break

                # Fuzzy match
                result = process.extractOne(col_lower, candidates, scorer=fuzz.ratio)
                if result and result[1] > best_score:
                    best_score = result[1]
                    best_target = target_field

            if best_target and best_score >= 50:  # Lower threshold for suggestions
                suggestions[col] = {
                    "target": best_target,
                    "confidence": best_score,
                    "auto_mapped": best_score >= FileIngestorAdapter.FUZZY_THRESHOLD
                }
                if best_score >= FileIngestorAdapter.FUZZY_THRESHOLD:
                    used_targets.add(best_target)

        return suggestions
