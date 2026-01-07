"""Pydantic models for normalized tax lien data."""

from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, computed_field, field_validator


SUPPORTED_STATES = ["IL", "FL", "AZ", "NJ", "IN", "CO", "IA", "MS", "AL", "SC"]


class SourcePlatform(str, Enum):
    """Enumeration of supported data source platforms."""
    REALAUCTION = "RealAuction"
    ZEUS = "Zeus"
    MANUAL_UPLOAD = "Manual Upload"
    UNKNOWN = "Unknown"


class TaxLien(BaseModel):
    """
    Normalized tax lien record.

    All data from any source (RealAuction, Zeus, CSV upload)
    gets transformed into this unified schema.
    """

    state: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="Two-character state code (e.g., 'FL', 'IL')"
    )
    county: str = Field(
        ...,
        min_length=1,
        description="County name"
    )
    parcel_id: str = Field(
        ...,
        min_length=1,
        description="Unique parcel identifier (APN/PIN)"
    )
    address: Optional[str] = Field(
        default=None,
        description="Property street address"
    )
    assessed_value: Optional[float] = Field(
        default=None,
        ge=0,
        description="County assessed value of the property"
    )
    face_amount: float = Field(
        ...,
        ge=0,
        description="Total tax amount owed (face value of lien)"
    )
    interest_rate_bid: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Winning bid interest rate (for auction results)"
    )
    auction_date: Optional[date] = Field(
        default=None,
        description="Date of the tax lien auction"
    )
    source_platform: SourcePlatform = Field(
        default=SourcePlatform.UNKNOWN,
        description="Platform where data was sourced"
    )
    raw_data: Optional[dict] = Field(
        default=None,
        description="Original raw data before normalization (for debugging)"
    )

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        """Ensure state code is uppercase and supported."""
        v = v.upper()
        if v not in SUPPORTED_STATES:
            raise ValueError(f"State '{v}' not in supported states: {SUPPORTED_STATES}")
        return v

    @field_validator("county")
    @classmethod
    def normalize_county(cls, v: str) -> str:
        """Normalize county name to title case."""
        return v.strip().title()

    @computed_field
    @property
    def lien_to_value_ratio(self) -> Optional[float]:
        """
        Calculate Lien-to-Value (LTV) ratio.

        This is the key risk metric: lower LTV = safer investment.
        A 5% LTV means the lien is only 5% of property value.
        """
        if self.assessed_value and self.assessed_value > 0:
            return round((self.face_amount / self.assessed_value) * 100, 2)
        return None

    @computed_field
    @property
    def equity_cushion(self) -> Optional[float]:
        """
        Calculate equity cushion percentage.

        100% - LTV = equity cushion. Higher = more protection.
        """
        if self.lien_to_value_ratio is not None:
            return round(100 - self.lien_to_value_ratio, 2)
        return None


class LienBatch(BaseModel):
    """A batch of tax liens from a single scrape/upload operation."""

    liens: list[TaxLien] = Field(default_factory=list)
    source_url: Optional[str] = Field(default=None)
    scrape_timestamp: Optional[date] = Field(default=None)
    state_filter: Optional[str] = Field(default=None)
    county_filter: Optional[str] = Field(default=None)

    @property
    def count(self) -> int:
        """Number of liens in the batch."""
        return len(self.liens)

    @property
    def total_face_amount(self) -> float:
        """Sum of all face amounts in batch."""
        return sum(lien.face_amount for lien in self.liens)

    @property
    def avg_ltv(self) -> Optional[float]:
        """Average LTV ratio across batch."""
        ltvs = [lien.lien_to_value_ratio for lien in self.liens if lien.lien_to_value_ratio]
        if ltvs:
            return round(sum(ltvs) / len(ltvs), 2)
        return None

    def filter_by_ltv(self, max_ltv: float) -> "LienBatch":
        """Return new batch with only liens below max LTV threshold."""
        filtered = [
            lien for lien in self.liens
            if lien.lien_to_value_ratio is not None
            and lien.lien_to_value_ratio <= max_ltv
        ]
        return LienBatch(
            liens=filtered,
            source_url=self.source_url,
            scrape_timestamp=self.scrape_timestamp,
            state_filter=self.state_filter,
            county_filter=self.county_filter
        )

    def filter_by_face_amount(self, min_amt: float, max_amt: float) -> "LienBatch":
        """Return new batch with liens in face amount range."""
        filtered = [
            lien for lien in self.liens
            if min_amt <= lien.face_amount <= max_amt
        ]
        return LienBatch(
            liens=filtered,
            source_url=self.source_url,
            scrape_timestamp=self.scrape_timestamp,
            state_filter=self.state_filter,
            county_filter=self.county_filter
        )
