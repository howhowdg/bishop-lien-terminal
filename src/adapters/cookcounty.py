"""Cook County adapter for Illinois tax lien auctions."""

import re
from datetime import date
from typing import Optional, Dict

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# Illinois county info
# Note: IL primarily uses Cook County as the main auction site
# Other counties have separate processes
IL_COUNTIES = {
    "cook": {
        "name": "Cook",
        "url": "https://www.cooktaxsale.com",
        "treasurer_url": "https://www.cookcountytreasurer.com/annualtaxsale.aspx",
        "population": 5_275_541,
        "townships": [
            "Barrington", "Berwyn", "Bloom", "Bremen", "Calumet", "Cicero",
            "Elk Grove", "Evanston", "Hanover", "Lemont", "Leyden", "Lyons",
            "Maine", "New Trier", "Niles", "Northfield", "Norwood Park", "Oak Park",
            "Orland", "Palatine", "Palos", "Proviso", "Rich", "River Forest",
            "Riverside", "Schaumburg", "South Chicago", "Stickney", "Thornton",
            "Wheeling", "Worth"
        ]
    },
    "dupage": {
        "name": "DuPage",
        "url": "https://www.dupagecounty.gov/treasurer/",
        "population": 932_877,
    },
    "lake": {
        "name": "Lake",
        "url": "https://www.lakecountyil.gov/185/Treasurers-Office",
        "population": 714_342,
    },
    "will": {
        "name": "Will",
        "url": "https://www.willcountytreasurer.com/",
        "population": 696_355,
    },
    "kane": {
        "name": "Kane",
        "url": "https://www.kanecountytreasurer.org/",
        "population": 516_522,
    },
    "mchenry": {
        "name": "McHenry",
        "url": "https://www.mchenrycountyil.gov/county-government/departments-j-z/treasurer",
        "population": 310_229,
    },
}


class CookCountyAdapter(ScrapingSource):
    """
    Scraper for Cook County (Illinois) tax lien auctions.

    Cook County is the largest tax lien market in Illinois.
    Annual tax sale typically held in December at cooktaxsale.com.

    Key features:
    - Delinquent property lists available for purchase ($250)
    - Online auction via cooktaxsale.com
    - PIN (Property Index Number) based identification
    - Township-based sale schedule

    Note: Full data requires paid access or registration.
    """

    platform = SourcePlatform.REALAUCTION
    supported_states = ["IL"]
    requires_auth = True
    base_url = "https://www.cooktaxsale.com"

    # Illinois statutory max interest (18% penalty + fees)
    MAX_INTEREST_RATE = 18.0
    REDEMPTION_PERIOD_YEARS = 2.5  # 2.5 years for residential

    def __init__(
        self,
        state: str = "IL",
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
        credentials: Optional[Dict[str, str]] = None,
    ):
        super().__init__(state, county, headless, timeout)
        self.credentials = credentials
        self.county_slug = self._get_county_slug()

    def _get_county_slug(self) -> str:
        """Convert county name to slug."""
        if not self.county:
            return "cook"  # Default

        slug = self.county.lower().strip()
        if slug in IL_COUNTIES:
            return slug

        # Try fuzzy match
        for key in IL_COUNTIES:
            if slug in key or key in slug:
                return key

        return "cook"

    def get_available_counties(self) -> list[str]:
        """Get list of Illinois counties with known info."""
        return [info["name"] for info in IL_COUNTIES.values()]

    def get_county_info(self) -> Dict:
        """Get info about the configured county."""
        return IL_COUNTIES.get(self.county_slug, {})

    def get_townships(self) -> list[str]:
        """Get townships for Cook County."""
        county_info = IL_COUNTIES.get(self.county_slug, {})
        return county_info.get("townships", [])

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
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

    async def fetch(self, max_records: int = 500, **kwargs) -> LienBatch:
        """
        Fetch tax lien data from Cook County Tax Sale.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with normalized TaxLien records

        Note:
            Full delinquent lists require $250 purchase from Cook County Treasurer.
            This adapter attempts to scrape publicly available information.
        """
        liens = []
        county_info = self.get_county_info()
        url = county_info.get("url", self.base_url)
        treasurer_url = county_info.get("treasurer_url", "")

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                # Try the tax sale site first
                print(f"Navigating to {url}...")

                response = await page.goto(url, wait_until="domcontentloaded")

                if response and response.status in [403, 401]:
                    print(f"Access denied. Registration required at {url}")
                else:
                    await page.wait_for_timeout(3000)
                    html = await page.content()
                    liens = self._parse_auction_page(html)

                # If no data, try treasurer's site
                if not liens and treasurer_url:
                    print(f"Trying treasurer site: {treasurer_url}")
                    await page.goto(treasurer_url, wait_until="networkidle")
                    await page.wait_for_timeout(2000)
                    html = await page.content()
                    info = self._parse_treasurer_page(html)

                    if info:
                        print(f"Sale info found: {info}")

                if liens:
                    print(f"Found {len(liens)} liens")
                else:
                    print(f"No public property data available.")
                    print(f"Cook County Treasurer offers delinquent lists for $250:")
                    print(f"  - Delinquent General Real Estate Tax List: $250")
                    print(f"  - Delinquent Special Assessment List: $250")
                    print(f"Contact: {treasurer_url}")

            except Exception as e:
                print(f"Cook County scraping error: {e}")
                print(f"Note: Visit {treasurer_url} for tax sale information.")

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
        """Parse Cook County Tax Sale page into TaxLien records."""
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

                # Detect header row - Cook County uses PIN
                header_keywords = ["pin", "parcel", "address", "amount", "township", "volume"]
                if any(any(kw in t.lower() for kw in header_keywords) for t in cell_texts):
                    headers = [t.lower() for t in cell_texts]
                    continue

                if len(cells) < 3 or not headers:
                    continue

                try:
                    data = dict(zip(headers, cell_texts))

                    # Cook County uses 14-digit PIN (Property Index Number)
                    parcel_id = self._find_field(data, ["pin", "parcel", "index"])
                    if not parcel_id:
                        continue

                    lien = TaxLien(
                        state="IL",
                        county=self.county or "Cook",
                        parcel_id=parcel_id,
                        address=self._find_field(data, ["address", "location", "property"]),
                        assessed_value=self._parse_currency(
                            self._find_field(data, ["assessed", "value"])
                        ),
                        face_amount=self._parse_currency(
                            self._find_field(data, ["amount", "due", "total", "tax", "delinquent"])
                        ) or 0.0,
                        interest_rate_bid=self.MAX_INTEREST_RATE,
                        auction_date=None,
                        source_platform=SourcePlatform.REALAUCTION,
                        raw_data={
                            **data,
                            "township": self._find_field(data, ["township"]),
                        }
                    )
                    liens.append(lien)
                except Exception:
                    continue

        return liens

    def _parse_treasurer_page(self, html: str) -> Dict:
        """Parse treasurer page for sale schedule and info."""
        soup = BeautifulSoup(html, "lxml")
        info = {}

        # Look for sale dates and schedule
        text = soup.get_text()

        # Try to find sale date
        date_patterns = [
            r"(\w+ \d+, \d{4})",
            r"(\d{1,2}/\d{1,2}/\d{4})",
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match and any(kw in text[max(0, match.start()-50):match.end()+50].lower()
                            for kw in ["sale", "auction", "begin"]):
                info["sale_date"] = match.group(1)
                break

        # Look for townships schedule
        townships = self.get_townships()
        for township in townships:
            if township.lower() in text.lower():
                if "schedule" not in info:
                    info["schedule"] = []
                info["schedule"].append(township)

        return info

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

    @classmethod
    def get_all_counties(cls) -> Dict[str, Dict]:
        """Get all known Illinois counties with tax sale info."""
        return IL_COUNTIES.copy()
