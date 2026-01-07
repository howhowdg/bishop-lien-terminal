"""Colorado Tax Sale adapter for CO county tax lien auctions."""

import re
from datetime import date
from typing import Optional, Dict

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# Colorado counties and their tax sale platforms
# Colorado uses a mix of coloradotaxsale.com, zeusauction.com, and realauction.com
COLORADO_COUNTIES = {
    # Counties using coloradotaxsale.com
    "adams": {
        "name": "Adams",
        "url": "https://adams.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 519_572,
    },
    "arapahoe": {
        "name": "Arapahoe",
        "url": "https://arapahoe.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 656_590,
    },
    "boulder": {
        "name": "Boulder",
        "url": "https://boulder.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 330_758,
    },
    "denver": {
        "name": "Denver",
        "url": "https://denver.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 715_522,
    },
    "douglas": {
        "name": "Douglas",
        "url": "https://douglas.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 357_978,
    },
    "el paso": {
        "name": "El Paso",
        "url": "https://elpaso.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 730_395,
    },
    "jefferson": {
        "name": "Jefferson",
        "url": "https://jefferson.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 582_881,
    },
    "larimer": {
        "name": "Larimer",
        "url": "https://larimer.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 359_066,
    },
    "mesa": {
        "name": "Mesa",
        "url": "https://mesa.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 158_205,
    },
    "pueblo": {
        "name": "Pueblo",
        "url": "https://pueblo.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 168_424,
    },
    "weld": {
        "name": "Weld",
        "url": "https://weld.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 328_981,
    },
    # Counties using Zeus/SRI
    "routt": {
        "name": "Routt",
        "url": "https://www.zeusauction.com",
        "platform": "zeus",
        "population": 25_638,
    },
    # Counties using RealAuction
    "archuleta": {
        "name": "Archuleta",
        "url": "https://archuleta.coloradotaxsale.com",
        "platform": "realauction",
        "population": 14_029,
    },
    "garfield": {
        "name": "Garfield",
        "url": "https://garfield.coloradotaxsale.com",
        "platform": "realauction",
        "population": 60_061,
    },
    "morgan": {
        "name": "Morgan",
        "url": "https://morgan.coloradotaxsale.com",
        "platform": "coloradotaxsale",
        "population": 29_068,
    },
    "elbert": {
        "name": "Elbert",
        "url": None,  # Live auction
        "platform": "live",
        "population": 27_921,
    },
    "san juan": {
        "name": "San Juan",
        "url": None,  # Live auction
        "platform": "live",
        "population": 728,
    },
}

# Additional smaller counties
ADDITIONAL_CO_COUNTIES = [
    "Alamosa", "Baca", "Bent", "Broomfield", "Chaffee", "Cheyenne", "Clear Creek",
    "Conejos", "Costilla", "Crowley", "Custer", "Delta", "Dolores", "Eagle",
    "Fremont", "Gilpin", "Grand", "Gunnison", "Hinsdale", "Huerfano", "Jackson",
    "Kiowa", "Kit Carson", "Lake", "La Plata", "Las Animas", "Lincoln", "Logan",
    "Mineral", "Moffat", "Montezuma", "Montrose", "Otero", "Ouray", "Park",
    "Phillips", "Pitkin", "Prowers", "Rio Blanco", "Rio Grande", "Saguache",
    "San Miguel", "Sedgwick", "Summit", "Teller", "Washington", "Yuma"
]


class ColoradoTaxSaleAdapter(ScrapingSource):
    """
    Scraper for Colorado Tax Sale - county-based tax lien auctions.

    Colorado uses multiple platforms for tax lien sales:
    - coloradotaxsale.com (most major counties)
    - zeusauction.com (some counties)
    - realauction.com (some counties)
    - Live in-person auctions (smaller counties)

    2025 Interest Rate: 14% (set by State Bank Commissioner)
    Redemption: 3 years, with secondary auction required for deed.

    Note: Requires registration to access full auction data.
    """

    platform = SourcePlatform.REALAUCTION
    supported_states = ["CO"]
    requires_auth = True
    base_url = "https://coloradotaxsale.com"

    # 2025 Colorado statutory interest rate (9% + federal discount rate)
    INTEREST_RATE_2025 = 14.0
    REDEMPTION_PERIOD_YEARS = 3

    def __init__(
        self,
        state: str = "CO",
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
            return "denver"  # Default to Denver

        slug = self.county.lower().replace(" ", "")
        if slug in COLORADO_COUNTIES:
            return slug

        # Try with space
        slug_with_space = self.county.lower()
        if slug_with_space in COLORADO_COUNTIES:
            return slug_with_space

        # Try fuzzy match
        for key in COLORADO_COUNTIES:
            if slug in key.replace(" ", "") or key.replace(" ", "") in slug:
                return key

        return "denver"

    def get_available_counties(self) -> list[str]:
        """Get list of Colorado counties with known tax sale info."""
        known = [info["name"] for info in COLORADO_COUNTIES.values()]
        return sorted(set(known + ADDITIONAL_CO_COUNTIES))

    def get_county_info(self) -> Dict:
        """Get info about the configured county."""
        return COLORADO_COUNTIES.get(self.county_slug, {})

    def get_county_url(self) -> str:
        """Get the auction URL for the configured county."""
        county_info = COLORADO_COUNTIES.get(self.county_slug, {})
        url = county_info.get("url")
        if url:
            return url
        # Default to coloradotaxsale subdomain
        return f"https://{self.county_slug.replace(' ', '')}.coloradotaxsale.com"

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
        Fetch tax lien data from Colorado Tax Sale.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with normalized TaxLien records

        Note:
            This platform typically returns 403 without registration.
            For live data, register at the county's tax sale site.
        """
        liens = []
        county_info = self.get_county_info()
        url = self.get_county_url()

        if county_info.get("platform") == "live":
            print(f"{county_info.get('name', self.county_slug.title())} County uses live in-person auctions.")
            print("Check county treasurer's website for auction dates and property lists.")
            return LienBatch(
                liens=[],
                source_url=url or "",
                scrape_timestamp=date.today(),
                state_filter=self.state,
                county_filter=self.county or self.county_slug.title()
            )

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                print(f"Navigating to {url}...")
                print(f"Platform: {county_info.get('platform', 'coloradotaxsale')}")

                response = await page.goto(url, wait_until="domcontentloaded")

                if response and response.status == 403:
                    print(f"Access denied (403). Registration required at {url}")
                    print(f"Colorado Tax Sale requires bidder registration.")
                    print(f"2025 Interest Rate: {self.INTEREST_RATE_2025}%")
                else:
                    await page.wait_for_timeout(3000)
                    html = await page.content()
                    liens = self._parse_auction_page(html)

                if liens:
                    print(f"Found {len(liens)} liens")
                else:
                    print(f"No public data available for {self.county_slug.title()} County.")
                    print(f"Register at {url} to access auction data.")

            except Exception as e:
                print(f"Colorado scraping error: {e}")
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
        """Parse Colorado Tax Sale auction page into TaxLien records."""
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
                if any("parcel" in t.lower() or "schedule" in t.lower() or "amount" in t.lower() for t in cell_texts):
                    headers = [t.lower() for t in cell_texts]
                    continue

                if len(cells) < 3 or not headers:
                    continue

                try:
                    data = dict(zip(headers, cell_texts))

                    parcel_id = self._find_field(data, ["parcel", "schedule", "account", "pin"])
                    if not parcel_id:
                        continue

                    lien = TaxLien(
                        state="CO",
                        county=self.county or self.county_slug.title(),
                        parcel_id=parcel_id,
                        address=self._find_field(data, ["address", "location", "situs", "property"]),
                        assessed_value=self._parse_currency(
                            self._find_field(data, ["assessed", "value", "actual"])
                        ),
                        face_amount=self._parse_currency(
                            self._find_field(data, ["amount", "due", "total", "tax", "delinquent"])
                        ) or 0.0,
                        interest_rate_bid=self.INTEREST_RATE_2025,
                        auction_date=None,
                        source_platform=SourcePlatform.REALAUCTION,
                        raw_data=data
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
    def get_all_counties(cls) -> Dict[str, Dict]:
        """Get all known Colorado counties with tax sale info."""
        return COLORADO_COUNTIES.copy()
