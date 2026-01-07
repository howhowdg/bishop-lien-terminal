"""Platform adapters for Tax Lien Terminal."""

from .base import LienSource, ScrapingSource, FileSource
from .realauction import RealAuctionAdapter
from .zeus import ZeusAdapter
from .file_ingestor import FileIngestorAdapter
from .lienhub import LienHubAdapter
from .govease import GovEaseAdapter
from .arizona_taxsale import ArizonaTaxSaleAdapter
from .nj_taxsale import NJTaxSaleAdapter
from .colorado_taxsale import ColoradoTaxSaleAdapter
from .sc_taxsale import SCTaxSaleAdapter
from .cookcounty import CookCountyAdapter

__all__ = [
    # Base classes
    "LienSource",
    "ScrapingSource",
    "FileSource",
    # Platform adapters
    "RealAuctionAdapter",
    "ZeusAdapter",
    "FileIngestorAdapter",
    "LienHubAdapter",
    "GovEaseAdapter",
    "ArizonaTaxSaleAdapter",
    "NJTaxSaleAdapter",
    "ColoradoTaxSaleAdapter",
    "SCTaxSaleAdapter",
    "CookCountyAdapter",
]
