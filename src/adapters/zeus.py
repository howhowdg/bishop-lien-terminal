"""Zeus Auction / SRI Services platform adapter for tax lien scraping."""

import re
from datetime import date
from typing import Optional

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# Zeus Auction site URLs by state/county
ZEUS_SITES = {
    "IN": {
        "marion": "https://www.zeusauction.com/",
        "lake": "https://www.zeusauction.com/",
        "hamilton": "https://www.zeusauction.com/",
    },
    "IA": {
        "polk": "https://www.zeusauction.com/",
        "linn": "https://www.zeusauction.com/",
    },
    "CO": {
        "adams": "https://www.zeusauction.com/",
        "jefferson": "https://www.zeusauction.com/",
    },
}


class ZeusAdapter(ScrapingSource):
    """
    Scraper for Zeus Auction / SRI Services tax lien platforms.

    Zeus is used primarily by Indiana, Iowa, and some Colorado counties
    for their tax sale auctions.

    Note: Zeus sites often have a unified portal where you select
    state/county, unlike RealAuction's separate subdomains.
    """

    platform = SourcePlatform.ZEUS
    supported_states = ["IN", "IA", "CO"]
    requires_auth = True  # Zeus typically requires registration
    base_url = "https://www.zeusauction.com/"

    # Column mappings from Zeus HTML to our schema
    COLUMN_MAPPINGS = {
        "parcel": "parcel_id",
        "parcel number": "parcel_id",
        "parcel #": "parcel_id",
        "key number": "parcel_id",
        "assessed value": "assessed_value",
        "assessed": "assessed_value",
        "total assessed": "assessed_value",
        "total due": "face_amount",
        "amount due": "face_amount",
        "minimum bid": "face_amount",
        "upset price": "face_amount",
        "property address": "address",
        "address": "address",
        "property location": "address",
    }

    def __init__(
        self,
        state: str,
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
        credentials: Optional[dict] = None
    ):
        """
        Initialize Zeus Auction scraper.

        Args:
            state: Two-character state code
            county: County name
            headless: Run browser headlessly
            timeout: Request timeout in ms
            credentials: Optional dict with 'username' and 'password'
        """
        super().__init__(state, county, headless, timeout)
        self.credentials = credentials

    def get_available_counties(self) -> list[str]:
        """Get list of counties with Zeus auctions for this state."""
        state_sites = ZEUS_SITES.get(self.state, {})
        return [c.title() for c in state_sites.keys()]

    async def fetch(self, max_pages: int = 5, **kwargs) -> LienBatch:
        """
        Fetch tax lien data from Zeus Auction.

        Args:
            max_pages: Maximum number of pagination pages to scrape

        Returns:
            LienBatch with normalized TaxLien records

        Raises:
            NotImplementedError: Zeus requires authentication flow
        """
        liens = []

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                await page.goto(self.base_url)

                # Handle login if credentials provided
                if self.credentials:
                    await self._handle_login(page)

                # Navigate to state/county auction
                await self._select_auction(page)

                # Scrape paginated results
                for page_num in range(max_pages):
                    html = await page.content()
                    page_liens = self._parse_table(html)
                    liens.extend(page_liens)

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

    async def _handle_login(self, page) -> None:
        """
        Handle Zeus login flow.

        Zeus typically requires account registration to view auction lists.
        """
        if not self.credentials:
            return

        try:
            # Look for login form
            username_field = page.locator("input[name='username'], input[type='email']").first
            password_field = page.locator("input[name='password'], input[type='password']").first
            submit_btn = page.locator("button[type='submit'], input[type='submit']").first

            if await username_field.count() > 0:
                await username_field.fill(self.credentials.get("username", ""))
                await password_field.fill(self.credentials.get("password", ""))
                await submit_btn.click()
                await page.wait_for_load_state("networkidle")
        except Exception:
            pass

    async def _select_auction(self, page) -> None:
        """
        Navigate to the specific state/county auction.

        Zeus uses dropdown selectors for state and county.
        """
        try:
            # Select state
            state_select = page.locator("select#state, select[name='state']").first
            if await state_select.count() > 0:
                await state_select.select_option(self.state)
                await page.wait_for_timeout(1000)

            # Select county
            if self.county:
                county_select = page.locator("select#county, select[name='county']").first
                if await county_select.count() > 0:
                    await county_select.select_option(label=self.county.title())
                    await page.wait_for_timeout(1000)

            # Click search/view button
            search_btn = page.locator("button:has-text('Search'), button:has-text('View')").first
            if await search_btn.count() > 0:
                await search_btn.click()
                await page.wait_for_load_state("networkidle")

        except Exception:
            pass

    async def _goto_next_page(self, page) -> bool:
        """Navigate to next pagination page."""
        try:
            next_btn = page.locator("a:has-text('Next'), .pagination-next").first
            if await next_btn.count() > 0 and await next_btn.is_enabled():
                await next_btn.click()
                await page.wait_for_load_state("networkidle")
                return True
            return False
        except Exception:
            return False

    def _parse_table(self, html: str) -> list[TaxLien]:
        """Parse Zeus HTML table into TaxLien records."""
        soup = BeautifulSoup(html, "lxml")
        liens = []

        tables = soup.find_all("table", class_=re.compile(r"auction|results|list"))
        if not tables:
            tables = soup.find_all("table")

        for table in tables:
            headers = []
            header_row = table.find("thead", recursive=False)
            if header_row:
                headers = [
                    th.get_text(strip=True).lower()
                    for th in header_row.find_all(["th", "td"])
                ]
            else:
                first_row = table.find("tr")
                if first_row:
                    headers = [
                        cell.get_text(strip=True).lower()
                        for cell in first_row.find_all(["th", "td"])
                    ]

            # Map headers to our schema
            column_map = {}
            for idx, header in enumerate(headers):
                for pattern, field in self.COLUMN_MAPPINGS.items():
                    if pattern in header:
                        column_map[idx] = field
                        break

            # Parse data rows
            tbody = table.find("tbody") or table
            rows = tbody.find_all("tr")
            start_idx = 0 if table.find("thead") else 1

            for row in rows[start_idx:]:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                raw_data = {}
                for idx, cell in enumerate(cells):
                    if idx in column_map:
                        raw_data[column_map[idx]] = cell.get_text(strip=True)

                if "parcel_id" not in raw_data or not raw_data["parcel_id"]:
                    continue

                try:
                    lien = self._create_lien(raw_data)
                    liens.append(lien)
                except Exception:
                    continue

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
            interest_rate_bid=None,
            auction_date=None,
            source_platform=self.platform,
            raw_data=raw_data
        )

    @staticmethod
    def _parse_currency(value: Optional[str]) -> Optional[float]:
        """Parse currency string to float."""
        if not value:
            return None
        try:
            cleaned = re.sub(r"[$,\s]", "", value)
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None
