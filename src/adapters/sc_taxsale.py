"""South Carolina Tax Sale adapter for SC county tax deed auctions."""

import re
from datetime import date
from typing import Optional, Dict, List

from bs4 import BeautifulSoup

from .base import ScrapingSource
from ..models import TaxLien, LienBatch, SourcePlatform


# South Carolina counties and their tax sale info
# SC is a TAX DEED state (not tax lien) - purchaser gets deed after redemption period
SC_COUNTIES = {
    "charleston": {
        "name": "Charleston",
        "url": "https://www.charlestoncounty.org/departments/delinquent-tax/tax-sale.php",
        "auction_location": "North Charleston Coliseum",
        "population": 408_235,
    },
    "greenville": {
        "name": "Greenville",
        "url": "https://www.greenvillecounty.org/apps/taxsale/",
        "population": 523_542,
    },
    "richland": {
        "name": "Richland",
        "url": "https://www.richlandcountysc.gov/Property-Business/Taxes/Delinquent-Taxes/Tax-Sale",
        "population": 415_759,
    },
    "horry": {
        "name": "Horry",
        "url": "https://www.horrycounty.org/departments/delinquent-tax",
        "population": 354_081,
    },
    "lexington": {
        "name": "Lexington",
        "url": "https://lex-co.sc.gov/departments/treasurer",
        "population": 293_991,
    },
    "spartanburg": {
        "name": "Spartanburg",
        "url": "https://www.spartanburgcounty.org/175/Tax-Sale",
        "population": 327_997,
    },
    "york": {
        "name": "York",
        "url": "https://www.yorkcountygov.com/388/Tax-Sale",
        "population": 282_090,
    },
    "berkeley": {
        "name": "Berkeley",
        "url": "https://www.berkeleycountysc.gov/dept/delinquent/",
        "population": 227_907,
    },
    "dorchester": {
        "name": "Dorchester",
        "url": "https://www.dorchestercountysc.gov/government/departments-services/delinquent-tax",
        "population": 162_809,
    },
    "beaufort": {
        "name": "Beaufort",
        "url": "https://www.bcgov.net/departments/administrative/delinquent-tax/",
        "population": 192_122,
    },
    "anderson": {
        "name": "Anderson",
        "url": "https://www.andersoncountysc.org/delinquent-tax",
        "population": 203_718,
    },
    "aiken": {
        "name": "Aiken",
        "url": "https://www.aikencountysc.gov/DspSvc?qSvcID=26",
        "population": 170_872,
    },
    "florence": {
        "name": "Florence",
        "url": "https://www.florenceco.org/offices/delinquent-tax/",
        "population": 138_293,
    },
    "sumter": {
        "name": "Sumter",
        "url": "https://www.sumtercountysc.org/delinquent-tax",
        "population": 106_721,
    },
    "lancaster": {
        "name": "Lancaster",
        "url": "https://www.lancastercountysc.gov/198/Tax-Sale-Procedures",
        "population": 98_012,
    },
    "georgetown": {
        "name": "Georgetown",
        "url": "https://www.gtcounty.org/408/Tax-Sale",
        "population": 63_766,
    },
    "oconee": {
        "name": "Oconee",
        "url": "https://oconeesc.com/delinquent-tax/tax-sale-information",
        "population": 79_546,
    },
    "colleton": {
        "name": "Colleton",
        "url": "https://www.colletoncounty.org/delinquent-tax/tax-sale",
        "population": 38_067,
    },
}

# All 46 SC Counties
ALL_SC_COUNTIES = [
    "Abbeville", "Aiken", "Allendale", "Anderson", "Bamberg", "Barnwell",
    "Beaufort", "Berkeley", "Calhoun", "Charleston", "Cherokee", "Chester",
    "Chesterfield", "Clarendon", "Colleton", "Darlington", "Dillon", "Dorchester",
    "Edgefield", "Fairfield", "Florence", "Georgetown", "Greenville", "Greenwood",
    "Hampton", "Horry", "Jasper", "Kershaw", "Lancaster", "Laurens", "Lee",
    "Lexington", "Marion", "Marlboro", "McCormick", "Newberry", "Oconee",
    "Orangeburg", "Pickens", "Richland", "Saluda", "Spartanburg", "Sumter",
    "Union", "Williamsburg", "York"
]


class SCTaxSaleAdapter(ScrapingSource):
    """
    Scraper for South Carolina Tax Sales - county-based tax deed auctions.

    IMPORTANT: South Carolina is a TAX DEED state, not tax lien.
    - Purchaser buys the property itself (subject to redemption)
    - 12-month redemption period for real property
    - Interest: 3%, 6%, 9%, 12% (increases quarterly)
    - Most sales are in-person with prior online registration

    Note: Most SC counties do not have online auction platforms.
    Data typically available via county delinquent tax office websites.
    """

    platform = SourcePlatform.MANUAL_UPLOAD
    supported_states = ["SC"]
    requires_auth = False  # Most info is public, but registration required to bid
    base_url = "https://dor.sc.gov"

    REDEMPTION_PERIOD_MONTHS = 12
    INTEREST_RATES = [3, 6, 9, 12]  # Quarterly increases

    def __init__(
        self,
        state: str = "SC",
        county: Optional[str] = None,
        headless: bool = True,
        timeout: Optional[int] = None,
    ):
        super().__init__(state, county, headless, timeout)
        self.county_slug = self._get_county_slug()

    def _get_county_slug(self) -> str:
        """Convert county name to slug."""
        if not self.county:
            return "charleston"  # Default

        slug = self.county.lower().strip()
        if slug in SC_COUNTIES:
            return slug

        # Try fuzzy match
        for key in SC_COUNTIES:
            if slug in key or key in slug:
                return key

        return "charleston"

    def get_available_counties(self) -> list[str]:
        """Get list of SC counties."""
        return ALL_SC_COUNTIES

    def get_county_info(self) -> Dict:
        """Get info about the configured county."""
        return SC_COUNTIES.get(self.county_slug, {})

    def get_county_url(self) -> str:
        """Get the tax sale info URL for the configured county."""
        county_info = SC_COUNTIES.get(self.county_slug, {})
        return county_info.get("url", "")

    async def _init_browser(self):
        """Initialize Playwright browser."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"]
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

    async def fetch(self, max_records: int = 500, **kwargs) -> LienBatch:
        """
        Fetch tax sale data from South Carolina county websites.

        Args:
            max_records: Maximum number of records to fetch

        Returns:
            LienBatch with property data (if available publicly)

        Note:
            SC counties primarily use in-person auctions.
            This adapter scrapes public delinquent property lists where available.
        """
        liens = []
        county_info = self.get_county_info()
        url = self.get_county_url()

        if not url:
            print(f"No known tax sale URL for {self.county or 'default'} County, SC")
            print(f"Contact {self.county_slug.title()} County Delinquent Tax Office for sale information.")
            return LienBatch(
                liens=[],
                source_url="",
                scrape_timestamp=date.today(),
                state_filter=self.state,
                county_filter=self.county or self.county_slug.title()
            )

        async with self:
            page = await self._context.new_page()
            page.set_default_timeout(self.timeout)

            try:
                print(f"Navigating to {url}...")
                print(f"Note: SC is a TAX DEED state (not tax lien)")

                await page.goto(url, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                html = await page.content()
                liens = self._parse_county_page(html)

                if liens:
                    print(f"Found {len(liens)} properties")
                else:
                    print(f"No property list found on page.")
                    print(f"SC counties typically post lists 3 weeks before sale.")
                    print(f"Check {url} closer to sale date.")

                    # Look for downloadable files
                    download_links = await self._find_download_links(page)
                    if download_links:
                        print(f"Found downloadable lists: {download_links}")

            except Exception as e:
                print(f"SC scraping error: {e}")

            finally:
                await page.close()

        return LienBatch(
            liens=liens[:max_records],
            source_url=url,
            scrape_timestamp=date.today(),
            state_filter=self.state,
            county_filter=self.county or self.county_slug.title()
        )

    async def _find_download_links(self, page) -> List[str]:
        """Find links to downloadable property lists."""
        links = []
        try:
            # Look for PDF, Excel, CSV links
            download_elements = await page.query_selector_all("a[href*='.pdf'], a[href*='.xlsx'], a[href*='.csv'], a[href*='download']")
            for elem in download_elements[:5]:  # Limit to first 5
                href = await elem.get_attribute("href")
                text = await elem.inner_text()
                if href and any(kw in text.lower() for kw in ["list", "sale", "delinquent", "property"]):
                    links.append(href)
        except Exception:
            pass
        return links

    def _parse_county_page(self, html: str) -> list[TaxLien]:
        """Parse SC county delinquent tax page into TaxLien records."""
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
                header_keywords = ["tms", "parcel", "map", "address", "amount", "owner", "property"]
                if any(any(kw in t.lower() for kw in header_keywords) for t in cell_texts):
                    headers = [t.lower() for t in cell_texts]
                    continue

                if len(cells) < 3 or not headers:
                    continue

                try:
                    data = dict(zip(headers, cell_texts))

                    # SC uses TMS (Tax Map System) numbers
                    parcel_id = self._find_field(data, ["tms", "parcel", "map", "pin", "key"])
                    if not parcel_id:
                        continue

                    lien = TaxLien(
                        state="SC",
                        county=self.county or self.county_slug.title(),
                        parcel_id=parcel_id,
                        address=self._find_field(data, ["address", "location", "property"]),
                        assessed_value=self._parse_currency(
                            self._find_field(data, ["assessed", "value", "appraised"])
                        ),
                        face_amount=self._parse_currency(
                            self._find_field(data, ["amount", "due", "total", "tax", "opening"])
                        ) or 0.0,
                        interest_rate_bid=None,  # SC uses tiered interest
                        auction_date=None,
                        source_platform=SourcePlatform.MANUAL_UPLOAD,
                        raw_data={
                            **data,
                            "state_type": "tax_deed",
                            "redemption_months": self.REDEMPTION_PERIOD_MONTHS,
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
