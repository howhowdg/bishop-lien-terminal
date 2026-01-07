"""Arizona Tax Sale adapter for AZ county tax lien auctions."""

import re
from datetime import date
from typing import Optional, Dict

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# Arizona counties and their tax sale platforms
ARIZONA_COUNTIES = {
    "maricopa": {
        "name": "Maricopa",
        "url": "https://maricopa.arizonataxsale.com",
        "treasurer_url": "https://treasurer.maricopa.gov/taxliens/",
        "population": 4_420_568,
    },
    "pima": {
        "name": "Pima",
        "url": "https://pima.arizonataxsale.com",
        "treasurer_url": "https://www.pima.gov/government/departments/finance-and-risk-management/treasurer/",
        "population": 1_043_433,
    },
    "pinal": {
        "name": "Pinal",
        "url": "https://pinal.arizonataxsale.com",
        "treasurer_url": "https://www.pinalcountyaz.gov/Treasurer/",
        "population": 462_789,
    },
    "yavapai": {
        "name": "Yavapai",
        "url": "https://yavapai.arizonataxsale.com",
        "treasurer_url": "https://www.yavapaiaz.gov/treasurer",
        "population": 236_209,
    },
    "mohave": {
        "name": "Mohave",
        "url": "https://mohave.arizonataxsale.com",
        "treasurer_url": "https://www.mohavecounty.us/ContentPage.aspx?id=120&page=22",
        "population": 213_267,
    },
    "yuma": {
        "name": "Yuma",
        "url": "https://yuma.arizonataxsale.com",
        "population": 203_881,
    },
    "coconino": {
        "name": "Coconino",
        "url": "https://coconino.arizonataxsale.com",
        "population": 145_101,
    },
    "cochise": {
        "name": "Cochise",
        "url": "https://cochise.arizonataxsale.com",
        "population": 126_279,
    },
    "navajo": {
        "name": "Navajo",
        "url": "https://navajo.arizonataxsale.com",
        "population": 110_924,
    },
    "apache": {
        "name": "Apache",
        "url": "https://apache.arizonataxsale.com",
        "population": 66_021,
    },
    "gila": {
        "name": "Gila",
        "url": "https://gila.arizonataxsale.com",
        "population": 54_018,
    },
    "santa cruz": {
        "name": "Santa Cruz",
        "url": "https://santacruz.arizonataxsale.com",
        "population": 47_669,
    },
    "graham": {
        "name": "Graham",
        "url": "https://graham.arizonataxsale.com",
        "population": 38_533,
    },
    "la paz": {
        "name": "La Paz",
        "url": "https://lapaz.arizonataxsale.com",
        "population": 16_557,
    },
    "greenlee": {
        "name": "Greenlee",
        "url": "https://greenlee.arizonataxsale.com",
        "population": 9_563,
    },
}


class ArizonaTaxSaleAdapter(ScrapingSource):
    """
    Scraper for Arizona Tax Sale - statewide tax lien auction platform.

    Arizona uses arizonataxsale.com subdomains for each county.
    Tax lien certificates earn up to 16% interest.
    Auctions typically held in February each year.

    Note: Requires registration as bidder to access full auction data.
    """

    platform = SourcePlatform.REALAUCTION
    supported_states = ["AZ"]
    requires_auth = True
    base_url = "https://arizonataxsale.com"

    # Arizona statutory max interest rate
    MAX_INTEREST_RATE = 16.0
    REDEMPTION_PERIOD_YEARS = 3

    def __init__(
        self,
        state: str = "AZ",
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
        credentials: Optional[Dict[str, str]] = None,
    ):
        super().__init__(state, county, headless, timeout)
        self.credentials = credentials
        self.county_slug = self._get_county_slug()

    def _get_county_slug(self) -> str:
        """Convert county name to URL slug."""
        if not self.county:
            return "maricopa"  # Default to largest county

        slug = self.county.lower().replace(" ", "")
        if slug in ARIZONA_COUNTIES:
            return slug

        # Try fuzzy match
        for key in ARIZONA_COUNTIES:
            if slug in key or key in slug:
                return key

        return "maricopa"

    def get_available_counties(self) -> list[str]:
        """Get list of Arizona counties with tax sale sites."""
        return [info["name"] for info in ARIZONA_COUNTIES.values()]

    def get_county_url(self) -> str:
        """Get the auction URL for the configured county."""
        county_info = ARIZONA_COUNTIES.get(self.county_slug, {})
        return county_info.get("url", f"https://{self.county_slug}.arizonataxsale.com")

    async def _init_browser(self):
        """Initialize Playwright browser with anti-detection settings."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ]
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        """)

    async def fetch(self, max_records: int = 500, **kwargs) -> LienBatch:
        """
        Fetch tax lien data from Arizona Tax Sale.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with normalized TaxLien records

        Note:
            This platform typically returns 403 without valid session/registration.
            For live data, register at arizonataxsale.com.
        """
        liens = []
        url = self.get_county_url()

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                print(f"Navigating to {url}...")

                response = await page.goto(url, wait_until="domcontentloaded")

                if response and response.status == 403:
                    print(f"Access denied (403). Registration required at {url}")
                    print("Arizona Tax Sale requires bidder registration to view auction data.")
                    print(f"Visit {url} to register for the {self.county_slug.title()} County auction.")
                else:
                    await page.wait_for_timeout(3000)
                    html = await page.content()
                    liens = self._parse_auction_page(html)

                if liens:
                    print(f"Found {len(liens)} liens")
                else:
                    # Try the treasurer's site for delinquent list
                    treasurer_url = ARIZONA_COUNTIES.get(self.county_slug, {}).get("treasurer_url")
                    if treasurer_url:
                        print(f"Trying treasurer site: {treasurer_url}")
                        await page.goto(treasurer_url, wait_until="networkidle")
                        html = await page.content()
                        liens = self._parse_treasurer_page(html)

            except Exception as e:
                print(f"Arizona scraping error: {e}")
                print(f"Note: Register at {url} to access auction data.")

            finally:
                await page.close()

        return LienBatch(
            liens=liens[:max_records],
            source_url=url,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=self.county or self.county_slug.title()
        )

    def _parse_auction_page(self, html: str) -> list[TaxLien]:
        """Parse Arizona Tax Sale auction page into TaxLien records."""
        soup = BeautifulSoup(html, "lxml")
        liens = []

        # Look for property tables
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            headers = []

            for row in rows:
                cells = row.find_all(["td", "th"])
                cell_texts = [c.get_text(strip=True) for c in cells]

                # Detect header row
                if any("parcel" in t.lower() or "amount" in t.lower() for t in cell_texts):
                    headers = [t.lower() for t in cell_texts]
                    continue

                if len(cells) < 3 or not headers:
                    continue

                try:
                    data = dict(zip(headers, cell_texts))
                    parcel_id = self._find_field(data, ["parcel", "account", "pin"])
                    if not parcel_id:
                        continue

                    lien = TaxLien(
                        state="AZ",
                        county=self.county or self.county_slug.title(),
                        parcel_id=parcel_id,
                        address=self._find_field(data, ["address", "location", "situs"]),
                        assessed_value=self._parse_currency(
                            self._find_field(data, ["assessed", "value", "full cash"])
                        ),
                        face_amount=self._parse_currency(
                            self._find_field(data, ["amount", "due", "total", "minimum"])
                        ) or 0.0,
                        interest_rate_bid=self.MAX_INTEREST_RATE,
                        auction_date=None,
                        source_platform=SourcePlatform.REALAUCTION,
                        raw_data=data
                    )
                    liens.append(lien)
                except Exception:
                    continue

        return liens

    def _parse_treasurer_page(self, html: str) -> list[TaxLien]:
        """Parse treasurer delinquent tax page for basic info."""
        soup = BeautifulSoup(html, "lxml")
        liens = []

        # Look for links to delinquent lists or maps
        links = soup.find_all("a", href=True)
        for link in links:
            text = link.get_text(strip=True).lower()
            if "delinquent" in text or "tax lien" in text:
                print(f"Found resource link: {link.get('href')}")

        return liens

    def _find_field(self, data: dict, keywords: list) -> Optional[str]:
        """Find a field value by keyword matching."""
        for key, value in data.items():
            for keyword in keywords:
                if keyword in key.lower():
                    return value
        return None

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
