"""Abstract base class for tax lien data sources (Strategy Pattern)."""

from abc import ABC, abstractmethod
from typing import Optional

from ..models import LienBatch, SourcePlatform


class LienSource(ABC):
    """
    Abstract base class for all tax lien data sources.

    Implements the Strategy Pattern - each subclass handles a specific
    platform/source (RealAuction, Zeus, CSV upload, etc.) but exposes
    a unified interface for fetching and normalizing data.
    """

    # Class-level attributes to be overridden by subclasses
    platform: SourcePlatform = SourcePlatform.UNKNOWN
    supported_states: list[str] = []
    requires_auth: bool = False

    def __init__(self, state: str, county: Optional[str] = None):
        """
        Initialize the data source.

        Args:
            state: Two-character state code (e.g., 'FL')
            county: Optional county name filter
        """
        self.state = state.upper()
        self.county = county
        self._validate_state()

    def _validate_state(self) -> None:
        """Ensure this adapter supports the requested state."""
        if self.supported_states and self.state not in self.supported_states:
            raise ValueError(
                f"{self.__class__.__name__} does not support state '{self.state}'. "
                f"Supported: {self.supported_states}"
            )

    @abstractmethod
    async def fetch(self, **kwargs) -> LienBatch:
        """
        Fetch and normalize tax lien data from the source.

        Returns:
            LienBatch containing normalized TaxLien records.

        Raises:
            ConnectionError: If unable to connect to data source
            ValueError: If data parsing fails
        """
        pass

    @abstractmethod
    def get_available_counties(self) -> list[str]:
        """
        Get list of available counties for the configured state.

        Returns:
            List of county names available on this platform.
        """
        pass

    @property
    def source_name(self) -> str:
        """Human-readable name for this source."""
        return f"{self.platform.value} - {self.state}"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(state={self.state}, county={self.county})>"


class ScrapingSource(LienSource):
    """
    Extended base class for sources that require web scraping.

    Adds common functionality for Playwright-based scrapers.
    """

    base_url: str = ""
    default_timeout: int = 30000  # 30 seconds

    def __init__(
        self,
        state: str,
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None
    ):
        """
        Initialize scraping source.

        Args:
            state: Two-character state code
            county: Optional county name filter
            headless: Whether to run browser in headless mode
            timeout: Request timeout in milliseconds
        """
        super().__init__(state, county)
        self.headless = headless
        self.timeout = timeout or self.default_timeout
        self._browser = None
        self._context = None

    async def _init_browser(self):
        """Initialize Playwright browser instance."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

    async def _close_browser(self):
        """Clean up browser resources."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if hasattr(self, "_playwright") and self._playwright:
            await self._playwright.stop()

    async def __aenter__(self):
        """Async context manager entry."""
        await self._init_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_browser()


class FileSource(LienSource):
    """
    Extended base class for file-based data sources.

    Handles CSV, Excel, and other file uploads.
    """

    supported_extensions: list[str] = [".csv", ".xlsx", ".xls"]

    def __init__(
        self,
        state: str,
        county: Optional[str] = None,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None
    ):
        """
        Initialize file source.

        Args:
            state: Two-character state code
            county: Optional county name
            file_path: Path to local file
            file_content: Raw file bytes (for uploads)
        """
        super().__init__(state, county)
        self.file_path = file_path
        self.file_content = file_content

    def get_available_counties(self) -> list[str]:
        """File sources don't have predefined county lists."""
        return []
