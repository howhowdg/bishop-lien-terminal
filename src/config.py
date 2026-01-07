"""Configuration and adapter registry for Tax Lien Terminal."""

from dataclasses import dataclass, field
from typing import Optional, Type

from .adapters import (
    LienSource,
    RealAuctionAdapter,
    ZeusAdapter,
    FileIngestorAdapter,
    LienHubAdapter,
    GovEaseAdapter,
    ArizonaTaxSaleAdapter,
    NJTaxSaleAdapter,
    ColoradoTaxSaleAdapter,
    SCTaxSaleAdapter,
    CookCountyAdapter,
)
from .models import SourcePlatform


@dataclass
class StateConfig:
    """Configuration for a single state's tax lien data sources."""

    state_code: str
    state_name: str
    primary_adapter: Type[LienSource]
    backup_adapters: list[Type[LienSource]] = field(default_factory=list)
    supports_file_upload: bool = True
    live_scraping: bool = False
    notes: str = ""


# Master registry of supported states and their adapters
STATE_REGISTRY: dict[str, StateConfig] = {
    "FL": StateConfig(
        state_code="FL",
        state_name="Florida",
        primary_adapter=LienHubAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=True,
        notes="Live scraping via LienHub. 30+ counties, year-round county-held liens."
    ),
    "AZ": StateConfig(
        state_code="AZ",
        state_name="Arizona",
        primary_adapter=ArizonaTaxSaleAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Uses arizonataxsale.com (registration required). 16% max rate. Feb auctions."
    ),
    "IL": StateConfig(
        state_code="IL",
        state_name="Illinois",
        primary_adapter=CookCountyAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Cook County via cooktaxsale.com (registration/$250 lists). Dec auctions."
    ),
    "NJ": StateConfig(
        state_code="NJ",
        state_name="New Jersey",
        primary_adapter=NJTaxSaleAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="565 municipalities, each runs own sale. newjerseytaxsale.com (registration required)."
    ),
    "IN": StateConfig(
        state_code="IN",
        state_name="Indiana",
        primary_adapter=ZeusAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Uses Zeus Auction (zeusauction.com). Registration required."
    ),
    "CO": StateConfig(
        state_code="CO",
        state_name="Colorado",
        primary_adapter=ColoradoTaxSaleAdapter,
        backup_adapters=[ZeusAdapter, FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Uses coloradotaxsale.com or Zeus. 14% rate (2025). Oct-Nov auctions."
    ),
    "IA": StateConfig(
        state_code="IA",
        state_name="Iowa",
        primary_adapter=GovEaseAdapter,
        backup_adapters=[ZeusAdapter, FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Uses GovEase or Zeus (registration required). 24% rate. June auctions."
    ),
    "MS": StateConfig(
        state_code="MS",
        state_name="Mississippi",
        primary_adapter=GovEaseAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Uses GovEase (govease.com). Premium bid auction. April/August sales."
    ),
    "AL": StateConfig(
        state_code="AL",
        state_name="Alabama",
        primary_adapter=GovEaseAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="Uses GovEase (govease.com). Interest rate bid-down (max 12%). March-June."
    ),
    "SC": StateConfig(
        state_code="SC",
        state_name="South Carolina",
        primary_adapter=SCTaxSaleAdapter,
        backup_adapters=[FileIngestorAdapter],
        supports_file_upload=True,
        live_scraping=False,
        notes="TAX DEED state (not lien). County websites. 12-month redemption. Nov-Dec."
    ),
}


# Platform -> States mapping for quick lookup
PLATFORM_STATES: dict[SourcePlatform, list[str]] = {
    SourcePlatform.REALAUCTION: ["FL", "AZ", "CO", "NJ", "IL"],
    SourcePlatform.ZEUS: ["IN", "IA", "CO"],
    SourcePlatform.MANUAL_UPLOAD: list(STATE_REGISTRY.keys()),
}


def get_adapter_for_state(
    state: str,
    county: Optional[str] = None,
    platform: Optional[SourcePlatform] = None,
    **kwargs
) -> LienSource:
    """
    Factory function to get the appropriate adapter for a state.

    Args:
        state: Two-character state code
        county: Optional county name
        platform: Force a specific platform (overrides default)
        **kwargs: Additional arguments passed to adapter constructor

    Returns:
        Configured LienSource adapter instance

    Raises:
        ValueError: If state not supported
    """
    state = state.upper()

    if state not in STATE_REGISTRY:
        raise ValueError(
            f"State '{state}' not supported. "
            f"Available: {list(STATE_REGISTRY.keys())}"
        )

    config = STATE_REGISTRY[state]

    # Determine which adapter to use
    if platform == SourcePlatform.MANUAL_UPLOAD:
        adapter_class = FileIngestorAdapter
    elif platform:
        # Find adapter matching requested platform
        for adapter in [config.primary_adapter] + config.backup_adapters:
            if hasattr(adapter, "platform") and adapter.platform == platform:
                adapter_class = adapter
                break
        else:
            adapter_class = config.primary_adapter
    else:
        adapter_class = config.primary_adapter

    return adapter_class(state=state, county=county, **kwargs)


def get_available_platforms(state: str) -> list[SourcePlatform]:
    """
    Get list of available platforms for a state.

    Args:
        state: Two-character state code

    Returns:
        List of available SourcePlatform values
    """
    state = state.upper()

    if state not in STATE_REGISTRY:
        return []

    config = STATE_REGISTRY[state]
    platforms = []

    # Add primary adapter platform
    if hasattr(config.primary_adapter, "platform"):
        platforms.append(config.primary_adapter.platform)

    # Add backup adapter platforms
    for adapter in config.backup_adapters:
        if hasattr(adapter, "platform"):
            platforms.append(adapter.platform)

    # Always include manual upload if supported
    if config.supports_file_upload and SourcePlatform.MANUAL_UPLOAD not in platforms:
        platforms.append(SourcePlatform.MANUAL_UPLOAD)

    return platforms


def get_counties_for_state(state: str, platform: Optional[SourcePlatform] = None) -> list[str]:
    """
    Get available counties for a state/platform combination.

    Args:
        state: Two-character state code
        platform: Optional platform filter

    Returns:
        List of county names
    """
    try:
        adapter = get_adapter_for_state(state, platform=platform)
        return adapter.get_available_counties()
    except Exception:
        return []


def is_live_scraping_available(state: str) -> bool:
    """Check if live scraping is available for a state."""
    state = state.upper()
    if state not in STATE_REGISTRY:
        return False
    return STATE_REGISTRY[state].live_scraping


def get_state_notes(state: str) -> str:
    """Get notes/info about a state's tax lien system."""
    state = state.upper()
    if state not in STATE_REGISTRY:
        return ""
    return STATE_REGISTRY[state].notes


# Investment metrics configuration
@dataclass
class InvestmentMetrics:
    """Configuration for investment analysis thresholds."""

    # LTV thresholds
    excellent_ltv: float = 5.0   # Below 5% LTV = excellent
    good_ltv: float = 10.0      # Below 10% = good
    acceptable_ltv: float = 20.0  # Below 20% = acceptable
    risky_ltv: float = 50.0     # Above 50% = high risk

    # Face amount ranges for filtering
    min_face_amount: float = 100.0
    max_face_amount: float = 50000.0

    # Interest rate thresholds (for auction results)
    excellent_rate: float = 18.0  # 18% = max statutory in many states
    good_rate: float = 12.0
    poor_rate: float = 5.0


# Default metrics instance
DEFAULT_METRICS = InvestmentMetrics()
