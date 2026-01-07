"""GovEase adapter for multi-state tax lien auctions."""

import re
from datetime import date
from typing import Optional, Dict, Any

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# States that use GovEase for tax lien auctions
GOVEASE_STATES = {
    "MS": {
        "name": "Mississippi",
        "counties": [
            "Adams", "Alcorn", "Amite", "Attala", "Benton", "Bolivar", "Calhoun",
            "Carroll", "Chickasaw", "Choctaw", "Claiborne", "Clarke", "Clay",
            "Coahoma", "Copiah", "Covington", "DeSoto", "Forrest", "Franklin",
            "George", "Greene", "Grenada", "Hancock", "Harrison", "Hinds", "Holmes",
            "Humphreys", "Issaquena", "Itawamba", "Jackson", "Jasper", "Jefferson",
            "Jefferson Davis", "Jones", "Kemper", "Lafayette", "Lamar", "Lauderdale",
            "Lawrence", "Leake", "Lee", "Leflore", "Lincoln", "Lowndes", "Madison",
            "Marion", "Marshall", "Monroe", "Montgomery", "Neshoba", "Newton",
            "Noxubee", "Oktibbeha", "Panola", "Pearl River", "Perry", "Pike",
            "Pontotoc", "Prentiss", "Quitman", "Rankin", "Scott", "Sharkey",
            "Simpson", "Smith", "Stone", "Sunflower", "Tallahatchie", "Tate",
            "Tippah", "Tishomingo", "Tunica", "Union", "Walthall", "Warren",
            "Washington", "Wayne", "Webster", "Wilkinson", "Winston", "Yalobusha", "Yazoo"
        ],
        "interest_rate": 1.5,  # 1.5% per month (18% annually)
        "redemption_months": 24,
        "auction_type": "premium_bid",
    },
    "AL": {
        "name": "Alabama",
        "counties": [
            "Autauga", "Baldwin", "Barbour", "Bibb", "Blount", "Bullock", "Butler",
            "Calhoun", "Chambers", "Cherokee", "Chilton", "Choctaw", "Clarke",
            "Clay", "Cleburne", "Coffee", "Colbert", "Conecuh", "Coosa", "Covington",
            "Crenshaw", "Cullman", "Dale", "Dallas", "DeKalb", "Elmore", "Escambia",
            "Etowah", "Fayette", "Franklin", "Geneva", "Greene", "Hale", "Henry",
            "Houston", "Jackson", "Jefferson", "Lamar", "Lauderdale", "Lawrence",
            "Lee", "Limestone", "Lowndes", "Macon", "Madison", "Marengo", "Marion",
            "Marshall", "Mobile", "Monroe", "Montgomery", "Morgan", "Perry",
            "Pickens", "Pike", "Randolph", "Russell", "Shelby", "St. Clair",
            "Sumter", "Talladega", "Tallapoosa", "Tuscaloosa", "Walker", "Washington",
            "Wilcox", "Winston"
        ],
        "interest_rate": 12.0,  # Max 12% annually (bid down)
        "redemption_months": 36,
        "auction_type": "interest_rate_bid_down",
    },
    "IA": {
        "name": "Iowa",
        "counties": [
            "Adair", "Adams", "Allamakee", "Appanoose", "Audubon", "Benton",
            "Black Hawk", "Boone", "Bremer", "Buchanan", "Buena Vista", "Butler",
            "Calhoun", "Carroll", "Cass", "Cedar", "Cerro Gordo", "Cherokee",
            "Chickasaw", "Clarke", "Clay", "Clayton", "Clinton", "Crawford",
            "Dallas", "Davis", "Decatur", "Delaware", "Des Moines", "Dickinson",
            "Dubuque", "Emmet", "Fayette", "Floyd", "Franklin", "Fremont", "Greene",
            "Grundy", "Guthrie", "Hamilton", "Hancock", "Hardin", "Harrison", "Henry",
            "Howard", "Humboldt", "Ida", "Iowa", "Jackson", "Jasper", "Jefferson",
            "Johnson", "Jones", "Keokuk", "Kossuth", "Lee", "Linn", "Louisa", "Lucas",
            "Lyon", "Madison", "Mahaska", "Marion", "Marshall", "Mills", "Mitchell",
            "Monona", "Monroe", "Montgomery", "Muscatine", "O'Brien", "Osceola",
            "Page", "Palo Alto", "Plymouth", "Pocahontas", "Polk", "Pottawattamie",
            "Poweshiek", "Ringgold", "Sac", "Scott", "Shelby", "Sioux", "Story",
            "Tama", "Taylor", "Union", "Van Buren", "Wapello", "Warren", "Washington",
            "Wayne", "Webster", "Winnebago", "Winneshiek", "Woodbury", "Worth", "Wright"
        ],
        "interest_rate": 24.0,  # 2% per month (24% annually)
        "redemption_months": 21,
        "auction_type": "standard",
    }
}


class GovEaseAdapter(ScrapingSource):
    """
    Scraper for GovEase - multi-state tax lien auction platform.

    GovEase handles tax lien sales for Mississippi, Alabama, Iowa, and other states.
    Most data requires bidder registration to access.

    Note: This adapter attempts to scrape public data. Full auction data
    requires registration at govease.com.
    """

    platform = SourcePlatform.MANUAL_UPLOAD  # Using this as placeholder
    supported_states = list(GOVEASE_STATES.keys())
    requires_auth = True  # Full data requires registration
    base_url = "https://www.govease.com"

    def __init__(
        self,
        state: str = "MS",
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
        credentials: Optional[Dict[str, str]] = None,
    ):
        super().__init__(state, county, headless, timeout)
        self.credentials = credentials
        self.state_config = GOVEASE_STATES.get(state.upper(), {})

    def get_available_counties(self) -> list[str]:
        """Get list of counties for the configured state."""
        return self.state_config.get("counties", [])

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

    async def _login(self, page) -> bool:
        """Attempt to login if credentials provided."""
        if not self.credentials:
            return False

        try:
            await page.goto(f"{self.base_url}/login", wait_until="networkidle")

            # Fill login form
            await page.fill('input[name="email"]', self.credentials.get("email", ""))
            await page.fill('input[name="password"]', self.credentials.get("password", ""))
            await page.click('button[type="submit"]')

            await page.wait_for_timeout(3000)
            return True
        except Exception as e:
            print(f"Login failed: {e}")
            return False

    async def fetch(self, max_records: int = 500, **kwargs) -> LienBatch:
        """
        Fetch tax lien data from GovEase.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with normalized TaxLien records

        Note:
            Without credentials, this returns limited public data.
            For full auction data, register at govease.com and provide credentials.
        """
        liens = []
        url = f"{self.base_url}/auctions"

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                # Try to login if credentials provided
                if self.credentials:
                    await self._login(page)

                print(f"Navigating to {url}...")
                await page.goto(url, wait_until="networkidle")
                await page.wait_for_timeout(3000)

                # Try to find auction listings
                html = await page.content()
                liens = self._parse_auction_list(html)

                if not liens:
                    print(f"No public data available. Registration required at govease.com")
                    print(f"State: {self.state} ({self.state_config.get('name', '')})")
                    print(f"Auction type: {self.state_config.get('auction_type', 'unknown')}")
                    print(f"Interest rate: {self.state_config.get('interest_rate', 'N/A')}%")

            except Exception as e:
                print(f"GovEase scraping error: {e}")
                print("Note: GovEase requires registration for auction data.")
                print(f"Visit {self.base_url} to register as a bidder.")

            finally:
                await page.close()

        return LienBatch(
            liens=liens[:max_records],
            source_url=url,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=self.county
        )

    def _parse_auction_list(self, html: str) -> list[TaxLien]:
        """Parse GovEase auction listings into TaxLien records."""
        soup = BeautifulSoup(html, "lxml")
        liens = []

        # Look for auction cards/rows
        auction_items = soup.find_all("div", class_=re.compile(r"auction|listing|property"))

        for item in auction_items:
            try:
                # Extract whatever data is publicly visible
                text = item.get_text(strip=True)

                # Try to extract parcel/property info
                parcel_match = re.search(r"parcel[:\s#]*(\w+)", text, re.I)
                amount_match = re.search(r"\$[\d,]+\.?\d*", text)

                if parcel_match or amount_match:
                    lien = TaxLien(
                        state=self.state,
                        county=self.county or "Unknown",
                        parcel_id=parcel_match.group(1) if parcel_match else "N/A",
                        address=None,
                        assessed_value=None,
                        face_amount=self._parse_currency(amount_match.group()) if amount_match else 0.0,
                        interest_rate_bid=self.state_config.get("interest_rate"),
                        auction_date=None,
                        source_platform=SourcePlatform.MANUAL_UPLOAD,
                        raw_data={"source": "govease", "raw_text": text[:500]}
                    )
                    liens.append(lien)

            except Exception:
                continue

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

    @classmethod
    def get_state_info(cls, state: str) -> Dict[str, Any]:
        """Get configuration info for a state."""
        return GOVEASE_STATES.get(state.upper(), {})
