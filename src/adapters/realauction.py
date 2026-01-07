"""RealAuction platform adapter for tax lien scraping."""

import re
from datetime import date
from typing import Optional

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# RealAuction site URLs by state/county
REALAUCTION_SITES = {
    "FL": {
        "duval": "https://duval.realtaxlien.com/",
        "hillsborough": "https://hillsborough.realtaxlien.com/",
        "orange": "https://orange.realtaxlien.com/",
        "demo": "https://demo.realtaxlien.com/",
    },
    "AZ": {
        "maricopa": "https://maricopa.realtaxlien.com/",
        "pima": "https://pima.realtaxlien.com/",
    },
    "CO": {
        "denver": "https://denver.realtaxlien.com/",
        "demo": "https://demo.realtaxlien.com/",
    },
    "NJ": {
        "demo": "https://demo.realtaxlien.com/",
    },
}


class RealAuctionAdapter(ScrapingSource):
    """
    Scraper for RealAuction tax lien platforms.

    RealAuction is used by many Florida, Arizona, Colorado, and New Jersey
    counties for their tax lien certificate auctions.
    """

    platform = SourcePlatform.REALAUCTION
    supported_states = ["FL", "AZ", "CO", "NJ"]
    requires_auth = False  # Public preview lists available without login

    # Column mappings from RealAuction HTML to our schema
    COLUMN_MAPPINGS = {
        "parcel": "parcel_id",
        "parcel id": "parcel_id",
        "certificate": "parcel_id",
        "cert #": "parcel_id",
        "assessed value": "assessed_value",
        "assessed": "assessed_value",
        "just value": "assessed_value",
        "face amount": "face_amount",
        "face value": "face_amount",
        "amount": "face_amount",
        "tax amount": "face_amount",
        "opening bid": "face_amount",
        "property address": "address",
        "address": "address",
        "situs address": "address",
        "location": "address",
        "interest rate": "interest_rate_bid",
        "rate": "interest_rate_bid",
        "bid rate": "interest_rate_bid",
    }

    def __init__(
        self,
        state: str,
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
        use_demo: bool = False
    ):
        """
        Initialize RealAuction scraper.

        Args:
            state: Two-character state code
            county: County name (required unless use_demo=True)
            headless: Run browser headlessly
            timeout: Request timeout in ms
            use_demo: Use demo site instead of real county site
        """
        super().__init__(state, county, headless, timeout)
        self.use_demo = use_demo
        self.base_url = self._get_site_url()

    def _get_site_url(self) -> str:
        """Determine the correct RealAuction URL for this state/county."""
        if self.use_demo:
            return "https://demo.realtaxlien.com/"

        state_sites = REALAUCTION_SITES.get(self.state, {})
        if self.county:
            county_key = self.county.lower().replace(" ", "")
            if county_key in state_sites:
                return state_sites[county_key]

        raise ValueError(
            f"No RealAuction site found for {self.state}/{self.county}. "
            f"Available: {list(state_sites.keys())}"
        )

    def get_available_counties(self) -> list[str]:
        """Get list of counties with RealAuction sites for this state."""
        state_sites = REALAUCTION_SITES.get(self.state, {})
        return [c.title() for c in state_sites.keys() if c != "demo"]

    async def fetch(self, max_pages: int = 5, **kwargs) -> LienBatch:
        """
        Fetch tax lien data from RealAuction.

        Args:
            max_pages: Maximum number of pagination pages to scrape

        Returns:
            LienBatch with normalized TaxLien records
        """
        liens = []

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                # Navigate to the auction list
                await page.goto(self.base_url)

                # Handle potential waiting room / splash screen
                await self._handle_splash_screen(page)

                # Navigate to the list view
                await self._navigate_to_list(page)

                # Scrape paginated results
                for page_num in range(max_pages):
                    html = await page.content()
                    page_liens = self._parse_table(html)
                    liens.extend(page_liens)

                    # Try to go to next page
                    if not await self._goto_next_page(page):
                        break

            finally:
                await page.close()

        return LienBatch(
            liens=liens,
            source_url=self.base_url,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=self.county
        )

    async def _handle_splash_screen(self, page) -> None:
        """Handle RealAuction's initial splash/terms screen."""
        try:
            # Look for common "Enter" or "Agree" buttons
            enter_selectors = [
                "button:has-text('Enter')",
                "button:has-text('Agree')",
                "button:has-text('Continue')",
                "input[type='submit'][value*='Enter']",
                "a:has-text('Enter Site')",
            ]
            for selector in enter_selectors:
                btn = page.locator(selector).first
                if await btn.count() > 0:
                    await btn.click()
                    await page.wait_for_load_state("networkidle")
                    return
        except Exception:
            pass  # No splash screen found, continue

    async def _navigate_to_list(self, page) -> None:
        """Navigate to the main auction list page."""
        try:
            # Look for "View List" or "Auction List" links
            list_selectors = [
                "a:has-text('View List')",
                "a:has-text('Auction List')",
                "a:has-text('Tax Sale List')",
                "a:has-text('Certificate List')",
                "#viewList",
                ".auction-list-link",
            ]
            for selector in list_selectors:
                link = page.locator(selector).first
                if await link.count() > 0:
                    await link.click()
                    await page.wait_for_load_state("networkidle")
                    return

            # If no link found, we might already be on the list page
            await page.wait_for_selector("table", timeout=5000)
        except Exception:
            pass

    async def _goto_next_page(self, page) -> bool:
        """
        Navigate to the next pagination page.

        Returns:
            True if successfully navigated, False if no more pages.
        """
        try:
            next_selectors = [
                "a:has-text('Next')",
                "button:has-text('Next')",
                ".pagination .next a",
                "a[rel='next']",
            ]
            for selector in next_selectors:
                btn = page.locator(selector).first
                if await btn.count() > 0 and await btn.is_enabled():
                    await btn.click()
                    await page.wait_for_load_state("networkidle")
                    return True
            return False
        except Exception:
            return False

    def _parse_table(self, html: str) -> list[TaxLien]:
        """
        Parse HTML table into TaxLien records.

        Args:
            html: Raw HTML content

        Returns:
            List of TaxLien objects
        """
        soup = BeautifulSoup(html, "lxml")
        liens = []

        # Find the main data table
        tables = soup.find_all("table")
        for table in tables:
            # Skip navigation/layout tables
            if not table.find("th"):
                continue

            # Extract headers
            headers = []
            header_row = table.find("tr")
            if header_row:
                headers = [
                    th.get_text(strip=True).lower()
                    for th in header_row.find_all(["th", "td"])
                ]

            # Map headers to our schema
            column_map = {}
            for idx, header in enumerate(headers):
                for pattern, field in self.COLUMN_MAPPINGS.items():
                    if pattern in header:
                        column_map[idx] = field
                        break

            # Parse data rows
            rows = table.find_all("tr")[1:]  # Skip header row
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                raw_data = {}
                for idx, cell in enumerate(cells):
                    if idx in column_map:
                        raw_data[column_map[idx]] = cell.get_text(strip=True)

                # Skip rows without parcel ID
                if "parcel_id" not in raw_data or not raw_data["parcel_id"]:
                    continue

                try:
                    lien = self._create_lien(raw_data)
                    liens.append(lien)
                except Exception:
                    continue  # Skip malformed rows

        return liens

    def _create_lien(self, raw_data: dict) -> TaxLien:
        """Create a TaxLien from parsed raw data."""
        return TaxLien(
            state=self.state,
            county=self.county or "Unknown",
            parcel_id=raw_data.get("parcel_id", ""),
            address=raw_data.get("address"),
            assessed_value=self._parse_currency(raw_data.get("assessed_value")),
            face_amount=self._parse_currency(raw_data.get("face_amount")) or 0.0,
            interest_rate_bid=self._parse_percentage(raw_data.get("interest_rate_bid")),
            auction_date=None,  # Would need to parse from page context
            source_platform=self.platform,
            raw_data=raw_data
        )

    @staticmethod
    def _parse_currency(value: Optional[str]) -> Optional[float]:
        """Parse currency string to float (e.g., '$1,234.56' -> 1234.56)."""
        if not value:
            return None
        try:
            # Remove currency symbols, commas, whitespace
            cleaned = re.sub(r"[$,\s]", "", value)
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_percentage(value: Optional[str]) -> Optional[float]:
        """Parse percentage string to float (e.g., '18%' -> 18.0)."""
        if not value:
            return None
        try:
            cleaned = re.sub(r"[%\s]", "", value)
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None
