"""New Jersey Tax Sale adapter for NJ municipality tax lien auctions."""

import re
from datetime import date
from typing import Optional, Dict, List

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# New Jersey municipalities with known tax sale sites
# NJ is unique - each of 565 municipalities runs its own sale
NJ_MUNICIPALITIES = {
    # Major cities/townships with online auctions
    "newark": {"name": "Newark", "url": "https://newark.newjerseytaxsale.com", "county": "Essex"},
    "jersey city": {"name": "Jersey City", "url": "https://jerseycity.newjerseytaxsale.com", "county": "Hudson"},
    "paterson": {"name": "Paterson", "url": "https://paterson.newjerseytaxsale.com", "county": "Passaic"},
    "elizabeth": {"name": "Elizabeth", "url": "https://elizabeth.newjerseytaxsale.com", "county": "Union"},
    "edison": {"name": "Edison", "url": "https://edison.newjerseytaxsale.com", "county": "Middlesex"},
    "woodbridge": {"name": "Woodbridge", "url": "https://woodbridge.newjerseytaxsale.com", "county": "Middlesex"},
    "toms river": {"name": "Toms River", "url": "https://tomsriver.newjerseytaxsale.com", "county": "Ocean"},
    "trenton": {"name": "Trenton", "url": "https://trenton.newjerseytaxsale.com", "county": "Mercer"},
    "clifton": {"name": "Clifton", "url": "https://clifton.newjerseytaxsale.com", "county": "Passaic"},
    "camden": {"name": "Camden", "url": "https://camden.newjerseytaxsale.com", "county": "Camden"},
    "passaic": {"name": "Passaic", "url": "https://passaic.newjerseytaxsale.com", "county": "Passaic"},
    "union city": {"name": "Union City", "url": "https://unioncity.newjerseytaxsale.com", "county": "Hudson"},
    "bayonne": {"name": "Bayonne", "url": "https://bayonne.newjerseytaxsale.com", "county": "Hudson"},
    "east orange": {"name": "East Orange", "url": "https://eastorange.newjerseytaxsale.com", "county": "Essex"},
    "vineland": {"name": "Vineland", "url": "https://vineland.newjerseytaxsale.com", "county": "Cumberland"},
    "new brunswick": {"name": "New Brunswick", "url": "https://newbrunswick.newjerseytaxsale.com", "county": "Middlesex"},
    "perth amboy": {"name": "Perth Amboy", "url": "https://perthamboy.newjerseytaxsale.com", "county": "Middlesex"},
    "plainfield": {"name": "Plainfield", "url": "https://plainfield.newjerseytaxsale.com", "county": "Union"},
    "irvington": {"name": "Irvington", "url": "https://irvington.newjerseytaxsale.com", "county": "Essex"},
    "hackensack": {"name": "Hackensack", "url": "https://hackensack.newjerseytaxsale.com", "county": "Bergen"},
    "kearny": {"name": "Kearny", "url": "https://kearny.newjerseytaxsale.com", "county": "Hudson"},
    "linden": {"name": "Linden", "url": "https://linden.newjerseytaxsale.com", "county": "Union"},
    "livingston": {"name": "Livingston", "url": "https://livingston.newjerseytaxsale.com", "county": "Essex"},
    "milltown": {"name": "Milltown", "url": "https://milltown.newjerseytaxsale.com", "county": "Middlesex"},
    "ewing": {"name": "Ewing", "url": "https://ewing.newjerseytaxsale.com", "county": "Mercer"},
    "teaneck": {"name": "Teaneck", "url": "https://teaneck.newjerseytaxsale.com", "county": "Bergen"},
    "willingboro": {"name": "Willingboro", "url": "https://willingboro.newjerseytaxsale.com", "county": "Burlington"},
}

# NJ Counties
NJ_COUNTIES = [
    "Atlantic", "Bergen", "Burlington", "Camden", "Cape May", "Cumberland",
    "Essex", "Gloucester", "Hudson", "Hunterdon", "Mercer", "Middlesex",
    "Monmouth", "Morris", "Ocean", "Passaic", "Salem", "Somerset",
    "Sussex", "Union", "Warren"
]


class NJTaxSaleAdapter(ScrapingSource):
    """
    Scraper for New Jersey Tax Sale - municipality-based tax lien auctions.

    NJ is unique in that each of its 565 municipalities conducts its own
    tax lien sale. Most use newjerseytaxsale.com with municipality subdomains.

    Tax lien certificates earn up to 18% interest (bid down from 18%).
    Redemption period is 2 years before foreclosure can begin.

    Note: Requires registration to access full auction data.
    """

    platform = SourcePlatform.REALAUCTION
    supported_states = ["NJ"]
    requires_auth = True
    base_url = "https://www.newjerseytaxsale.com"

    MAX_INTEREST_RATE = 18.0  # Bid down from 18%
    REDEMPTION_PERIOD_YEARS = 2

    def __init__(
        self,
        state: str = "NJ",
        county: Optional[str] = None,
        municipality: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
        credentials: Optional[Dict[str, str]] = None,
    ):
        super().__init__(state, county, headless, timeout)
        self.municipality = municipality
        self.credentials = credentials
        self.municipality_slug = self._get_municipality_slug()

    def _get_municipality_slug(self) -> Optional[str]:
        """Convert municipality name to URL slug."""
        if not self.municipality:
            return None

        slug = self.municipality.lower().strip()
        if slug in NJ_MUNICIPALITIES:
            return slug

        # Try fuzzy match
        for key in NJ_MUNICIPALITIES:
            if slug in key or key in slug:
                return key

        return None

    def get_available_counties(self) -> list[str]:
        """Get list of NJ counties."""
        return NJ_COUNTIES

    def get_municipalities_by_county(self, county: str) -> List[str]:
        """Get municipalities in a specific county."""
        county_lower = county.lower()
        return [
            info["name"] for slug, info in NJ_MUNICIPALITIES.items()
            if info.get("county", "").lower() == county_lower
        ]

    def get_municipality_url(self) -> str:
        """Get the auction URL for the configured municipality."""
        if self.municipality_slug:
            info = NJ_MUNICIPALITIES.get(self.municipality_slug, {})
            return info.get("url", f"https://{self.municipality_slug}.newjerseytaxsale.com")
        return self.base_url

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
        Fetch tax lien data from New Jersey Tax Sale.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with normalized TaxLien records

        Note:
            This platform returns 403 without valid registration.
            For live data, register at newjerseytaxsale.com.
        """
        liens = []
        url = self.get_municipality_url()

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                print(f"Navigating to {url}...")

                response = await page.goto(url, wait_until="domcontentloaded")

                if response and response.status == 403:
                    print(f"Access denied (403). Registration required at {url}")
                    print("New Jersey Tax Sale requires bidder registration to view auction data.")
                    if self.municipality_slug:
                        muni_info = NJ_MUNICIPALITIES.get(self.municipality_slug, {})
                        print(f"Municipality: {muni_info.get('name', self.municipality_slug.title())}")
                        print(f"County: {muni_info.get('county', 'Unknown')}")
                else:
                    await page.wait_for_timeout(3000)
                    html = await page.content()
                    liens = self._parse_auction_page(html)

                if liens:
                    print(f"Found {len(liens)} liens")
                else:
                    print("No public data available.")
                    print(f"NJ municipalities run individual sales - check specific municipality sites.")
                    print(f"Known municipalities: {len(NJ_MUNICIPALITIES)}")

            except Exception as e:
                print(f"NJ scraping error: {e}")
                print(f"Note: Register at {url} to access auction data.")

            finally:
                await page.close()

        county_name = None
        if self.municipality_slug:
            county_name = NJ_MUNICIPALITIES.get(self.municipality_slug, {}).get("county")

        return LienBatch(
            liens=liens[:max_records],
            source_url=url,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=county_name or self.county
        )

    def _parse_auction_page(self, html: str) -> list[TaxLien]:
        """Parse NJ Tax Sale auction page into TaxLien records."""
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
                if any("block" in t.lower() or "lot" in t.lower() or "amount" in t.lower() for t in cell_texts):
                    headers = [t.lower() for t in cell_texts]
                    continue

                if len(cells) < 3 or not headers:
                    continue

                try:
                    data = dict(zip(headers, cell_texts))

                    # NJ uses block/lot system
                    block = self._find_field(data, ["block"])
                    lot = self._find_field(data, ["lot"])
                    qualifier = self._find_field(data, ["qual", "qualifier"])

                    parcel_id = f"{block or ''}-{lot or ''}"
                    if qualifier:
                        parcel_id += f"-{qualifier}"

                    if not block and not lot:
                        parcel_id = self._find_field(data, ["parcel", "account", "pin"])

                    if not parcel_id or parcel_id == "-":
                        continue

                    county_name = None
                    if self.municipality_slug:
                        county_name = NJ_MUNICIPALITIES.get(self.municipality_slug, {}).get("county")

                    lien = TaxLien(
                        state="NJ",
                        county=county_name or self.county or "Unknown",
                        parcel_id=parcel_id.strip("-"),
                        address=self._find_field(data, ["address", "location", "property"]),
                        assessed_value=self._parse_currency(
                            self._find_field(data, ["assessed", "value"])
                        ),
                        face_amount=self._parse_currency(
                            self._find_field(data, ["amount", "due", "total", "delinquent"])
                        ) or 0.0,
                        interest_rate_bid=self.MAX_INTEREST_RATE,
                        auction_date=None,
                        source_platform=SourcePlatform.REALAUCTION,
                        raw_data={
                            **data,
                            "municipality": self.municipality_slug,
                        }
                    )
                    liens.append(lien)
                except Exception:
                    continue

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

    @classmethod
    def get_all_municipalities(cls) -> Dict[str, Dict]:
        """Get all known NJ municipalities with tax sale info."""
        return NJ_MUNICIPALITIES.copy()
