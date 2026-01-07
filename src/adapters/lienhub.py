"""LienHub adapter for Florida tax lien scraping."""

import re
from datetime import date
from typing import Optional

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# Florida counties on LienHub
LIENHUB_COUNTIES = [
    "alachua", "bay", "brevard", "broward", "charlotte", "citrus", "clay",
    "collier", "duval", "escambia", "flagler", "hernando", "hillsborough",
    "indianriver", "lake", "lee", "martin", "miamidade", "monroe", "nassau",
    "okaloosa", "orange", "osceola", "pasco", "pinellas", "santarosa",
    "seminole", "stlucie", "sumter", "volusia", "walton"
]


class LienHubAdapter(ScrapingSource):
    """
    Scraper for LienHub - Florida's primary tax lien certificate platform.

    LienHub is used by 30+ Florida counties for tax certificate sales.
    Supports both annual auction data and county-held (year-round) liens.
    """

    platform = SourcePlatform.REALAUCTION  # Using same enum for now
    supported_states = ["FL"]
    requires_auth = False  # Public data available without login
    base_url = "https://lienhub.com"

    def __init__(
        self,
        state: str = "FL",
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
    ):
        super().__init__(state, county, headless, timeout)
        self.county_slug = self._get_county_slug()

    def _get_county_slug(self) -> str:
        """Convert county name to LienHub URL slug."""
        if not self.county:
            return "duval"  # Default

        slug = self.county.lower().replace(" ", "").replace("-", "")
        if slug in LIENHUB_COUNTIES:
            return slug

        # Try fuzzy match
        for c in LIENHUB_COUNTIES:
            if slug in c or c in slug:
                return c

        return "duval"  # Fallback

    def get_available_counties(self) -> list[str]:
        """Get list of Florida counties on LienHub."""
        return [c.title() for c in LIENHUB_COUNTIES]

    async def _init_browser(self):
        """Initialize Playwright browser with anti-detection settings."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        # Remove webdriver detection
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

    async def fetch(self, max_records: int = 500, **kwargs) -> LienBatch:
        """
        Fetch tax lien data from LienHub.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with normalized TaxLien records
        """
        liens = []
        url = f"{self.base_url}/county/{self.county_slug}/countyheld/certificates"

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                print(f"Navigating to {url}...")
                await page.goto(url, wait_until="networkidle")
                await page.wait_for_timeout(2000)  # Let DataTables load

                # Get the page content
                html = await page.content()
                liens = self._parse_table(html)

                print(f"Found {len(liens)} liens on page")

                # Check if there are more pages (DataTables pagination)
                while len(liens) < max_records:
                    next_btn = page.locator("button.dt-paging-button:has-text('Next')")
                    if await next_btn.count() > 0:
                        is_disabled = await next_btn.get_attribute("disabled")
                        if is_disabled:
                            break
                        await next_btn.click()
                        await page.wait_for_timeout(1000)
                        html = await page.content()
                        new_liens = self._parse_table(html)
                        if not new_liens:
                            break
                        liens.extend(new_liens)
                        print(f"Total liens: {len(liens)}")
                    else:
                        break

            except Exception as e:
                print(f"Scraping error: {e}")
                raise

            finally:
                await page.close()

        return LienBatch(
            liens=liens[:max_records],
            source_url=url,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=self.county
        )

    def _parse_table(self, html: str) -> list[TaxLien]:
        """Parse LienHub DataTable into TaxLien records."""
        soup = BeautifulSoup(html, "lxml")
        liens = []

        # Find the data table
        table = soup.find("table", {"id": "cert_table"}) or soup.find("table")
        if not table:
            return liens

        # Parse rows from tbody
        tbody = table.find("tbody")
        if not tbody:
            return liens

        rows = tbody.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            try:
                # Extract data from cells
                account_num = cells[0].get_text(strip=True)
                tax_year = cells[1].get_text(strip=True)
                cert_num = cells[2].get_text(strip=True)
                issued_date = cells[3].get_text(strip=True)
                expiration_date = cells[4].get_text(strip=True)
                purchase_amt = cells[5].get_text(strip=True)

                # Parse purchase amount
                face_amount = self._parse_currency(purchase_amt)
                if face_amount is None:
                    face_amount = 0.0

                lien = TaxLien(
                    state="FL",
                    county=self.county or self.county_slug.title(),
                    parcel_id=account_num,
                    address=None,  # Not available in list view
                    assessed_value=None,  # Would need detail page
                    face_amount=face_amount,
                    interest_rate_bid=None,  # County-held liens are at statutory max (18%)
                    auction_date=self._parse_date(issued_date),
                    source_platform=SourcePlatform.REALAUCTION,
                    raw_data={
                        "account_number": account_num,
                        "tax_year": tax_year,
                        "certificate_number": cert_num,
                        "issued_date": issued_date,
                        "expiration_date": expiration_date,
                        "purchase_amount": purchase_amt,
                    }
                )
                liens.append(lien)

            except Exception as e:
                continue  # Skip malformed rows

        return liens

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

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        """Parse date string (YYYY-MM-DD format)."""
        if not value:
            return None
        try:
            parts = value.split("-")
            if len(parts) == 3:
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
            return None
        except (ValueError, TypeError):
            return None
