"""
Bishop Lien Terminal
Retro amber terminal aesthetic for tax lien data
"""

import asyncio
from datetime import date
from typing import Optional, List

import pandas as pd
import streamlit as st

from src.config import STATE_REGISTRY, DEFAULT_METRICS
from src.models import SourcePlatform, TaxLien, LienBatch
from src.adapters import FileIngestorAdapter, LienHubAdapter
from src.adapters.file_ingestor import ColumnMappingHelper

# Page configuration
st.set_page_config(
    page_title="BISHOP LIEN TERMINAL",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# AMBER TERMINAL THEME - Retro IBM Terminal Aesthetic
# ============================================================
TERMINAL_CSS = """
<style>
    /* Import IBM Plex Mono for authentic terminal feel */
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&display=swap');

    /* Color palette */
    :root {
        --bg-primary: #0a0a0a;
        --bg-secondary: #111111;
        --bg-tertiary: #1a1a1a;
        --amber: #ffb000;
        --amber-dim: #cc8800;
        --amber-bright: #ffc832;
        --amber-glow: rgba(255, 176, 0, 0.15);
        --text-primary: #ffb000;
        --text-secondary: #aa7700;
        --border-color: #3d3000;
    }

    /* Global overrides */
    .stApp {
        background-color: var(--bg-primary);
        font-family: 'IBM Plex Mono', monospace;
    }

    /* Main container */
    .main .block-container {
        background-color: var(--bg-primary);
        padding-top: 2rem;
        max-width: 100%;
    }

    /* All text amber */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: var(--text-primary) !important;
        font-family: 'IBM Plex Mono', monospace !important;
    }

    /* Headers with glow effect */
    h1, h2, h3 {
        color: var(--amber-bright) !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-weight: 700 !important;
        text-shadow: 0 0 10px var(--amber-glow), 0 0 20px var(--amber-glow);
        letter-spacing: 2px;
    }

    h1 {
        font-size: 2rem !important;
        border-bottom: 2px solid var(--amber-dim);
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem !important;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: var(--bg-secondary) !important;
        border-right: 1px solid var(--border-color);
    }

    [data-testid="stSidebar"] .stMarkdown {
        color: var(--text-primary);
    }

    /* Sidebar title */
    [data-testid="stSidebar"] h1 {
        font-size: 1.1rem !important;
        letter-spacing: 3px;
        text-transform: uppercase;
        border-bottom: 1px solid var(--amber-dim);
    }

    /* Metric cards - terminal style boxes */
    [data-testid="stMetric"] {
        background-color: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 0 !important;
        padding: 1rem !important;
        box-shadow: inset 0 0 20px rgba(255, 176, 0, 0.03);
    }

    [data-testid="stMetric"] label {
        color: var(--text-secondary) !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--amber-bright) !important;
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        text-shadow: 0 0 8px var(--amber-glow);
    }

    /* Buttons */
    .stButton > button {
        background-color: transparent !important;
        color: var(--amber) !important;
        border: 1px solid var(--amber) !important;
        border-radius: 0 !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-weight: 600 !important;
        letter-spacing: 1px;
        text-transform: uppercase;
        transition: all 0.2s ease;
    }

    .stButton > button:hover {
        background-color: var(--amber) !important;
        color: var(--bg-primary) !important;
        box-shadow: 0 0 15px var(--amber-glow);
    }

    .stButton > button[kind="primary"] {
        background-color: var(--amber) !important;
        color: var(--bg-primary) !important;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: var(--amber-bright) !important;
        box-shadow: 0 0 20px var(--amber-glow);
    }

    /* Selectbox and inputs */
    .stSelectbox > div > div,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input {
        background-color: var(--bg-tertiary) !important;
        color: var(--amber) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 0 !important;
        font-family: 'IBM Plex Mono', monospace !important;
    }

    .stSelectbox > div > div:focus,
    .stTextInput > div > div > input:focus {
        border-color: var(--amber) !important;
        box-shadow: 0 0 5px var(--amber-glow) !important;
    }

    /* Radio buttons */
    .stRadio > div {
        background-color: transparent !important;
    }

    .stRadio > div > label {
        color: var(--text-primary) !important;
    }

    /* Slider */
    .stSlider > div > div > div {
        background-color: var(--amber-dim) !important;
    }

    .stSlider > div > div > div > div {
        background-color: var(--amber) !important;
    }

    /* Dataframe / Table */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border-color) !important;
    }

    [data-testid="stDataFrame"] > div {
        background-color: var(--bg-secondary) !important;
    }

    /* DataFrame header */
    .dvn-scroller th {
        background-color: var(--bg-tertiary) !important;
        color: var(--amber) !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        font-size: 0.75rem !important;
        letter-spacing: 1px !important;
        border-bottom: 1px solid var(--amber-dim) !important;
    }

    /* DataFrame cells */
    .dvn-scroller td {
        background-color: var(--bg-secondary) !important;
        color: var(--text-primary) !important;
        font-size: 0.85rem !important;
        border-bottom: 1px solid var(--border-color) !important;
    }

    .dvn-scroller tr:hover td {
        background-color: var(--bg-tertiary) !important;
    }

    /* Info/Warning/Success boxes */
    .stAlert {
        background-color: var(--bg-tertiary) !important;
        border: 1px solid var(--amber-dim) !important;
        border-radius: 0 !important;
        color: var(--text-primary) !important;
    }

    /* Download button */
    .stDownloadButton > button {
        background-color: transparent !important;
        color: var(--amber) !important;
        border: 1px solid var(--amber-dim) !important;
    }

    /* File uploader */
    [data-testid="stFileUploader"] {
        background-color: var(--bg-tertiary) !important;
        border: 1px dashed var(--border-color) !important;
    }

    [data-testid="stFileUploader"]:hover {
        border-color: var(--amber) !important;
    }

    /* Multiselect */
    .stMultiSelect > div > div {
        background-color: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 0 !important;
    }

    .stMultiSelect [data-baseweb="tag"] {
        background-color: var(--amber-dim) !important;
        color: var(--bg-primary) !important;
    }

    /* Spinner */
    .stSpinner > div {
        border-top-color: var(--amber) !important;
    }

    /* Horizontal rule */
    hr {
        border-color: var(--border-color) !important;
    }

    /* Links */
    a {
        color: var(--amber) !important;
    }

    a:hover {
        color: var(--amber-bright) !important;
        text-shadow: 0 0 5px var(--amber-glow);
    }

    /* CRT Scanline effect (subtle) */
    .stApp::before {
        content: "";
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        pointer-events: none;
        background: repeating-linear-gradient(
            0deg,
            rgba(0, 0, 0, 0.1),
            rgba(0, 0, 0, 0.1) 1px,
            transparent 1px,
            transparent 2px
        );
        z-index: 9999;
        opacity: 0.3;
    }

    /* Terminal cursor blink for title */
    @keyframes blink {
        0%, 50% { opacity: 1; }
        51%, 100% { opacity: 0; }
    }

    .cursor {
        animation: blink 1s infinite;
        color: var(--amber);
    }

    /* Status indicator styling */
    .status-online {
        color: #00ff00;
        text-shadow: 0 0 5px #00ff00;
    }

    /* Welcome text */
    .welcome-text {
        line-height: 1.8;
        color: var(--text-secondary);
    }

    .welcome-text strong {
        color: var(--amber);
    }
</style>
"""

st.markdown(TERMINAL_CSS, unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    if "liens_data" not in st.session_state:
        st.session_state.liens_data = None
    if "last_fetch_time" not in st.session_state:
        st.session_state.last_fetch_time = None


def render_sidebar():
    """Render simplified sidebar."""
    st.sidebar.markdown("### BISHOP LIEN TERMINAL")
    st.sidebar.markdown("---")

    # Mode selection
    mode = st.sidebar.radio(
        "MODE",
        ["SCRAPE STATE", "UPLOAD FILE"],
    )

    st.sidebar.markdown("---")

    if mode == "SCRAPE STATE":
        render_state_scraper()
    else:
        render_upload_controls()

    # Filters (if data loaded)
    if st.session_state.liens_data is not None and st.session_state.liens_data.count > 0:
        st.sidebar.markdown("---")
        render_filter_controls()

    # Status footer
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        f"<small style='color: #666;'>SYS STATUS: <span class='status-online'>ONLINE</span><br>"
        f"VER 1.0.0 | {date.today().strftime('%Y.%m.%d')}</small>",
        unsafe_allow_html=True
    )


def render_state_scraper():
    """Simple state selector that scrapes all counties."""
    st.sidebar.markdown("**SELECT STATE**")

    # Only show states with live scraping
    scrape_states = ["FL"]

    state = st.sidebar.selectbox(
        "State",
        options=scrape_states,
        format_func=lambda x: f"{x} - {STATE_REGISTRY[x].state_name}",
        label_visibility="collapsed"
    )

    config = STATE_REGISTRY[state]
    st.sidebar.info(f"📡 {config.notes}")

    if st.sidebar.button("[ FETCH ALL LIENS ]", type="primary", use_container_width=True):
        with st.spinner(f"SCANNING {state} COUNTIES..."):
            try:
                liens = scrape_all_counties(state)
                st.session_state.liens_data = liens
                st.session_state.last_fetch_time = date.today()
                st.sidebar.success(f"✓ LOADED {liens.count} LIENS")
            except Exception as e:
                st.sidebar.error(f"ERROR: {str(e)}")


def render_upload_controls():
    """Render file upload controls."""
    st.sidebar.markdown("**UPLOAD DATA FILE**")

    state = st.sidebar.selectbox(
        "State",
        options=list(STATE_REGISTRY.keys()),
        format_func=lambda x: f"{x} - {STATE_REGISTRY[x].state_name}",
        key="upload_state",
        label_visibility="collapsed"
    )

    county = st.sidebar.text_input("COUNTY", placeholder="e.g., Cook")

    uploaded_file = st.sidebar.file_uploader(
        "CSV/XLSX FILE",
        type=["csv", "xlsx", "xls"],
    )

    if uploaded_file:
        if st.sidebar.button("[ PROCESS FILE ]", type="primary", use_container_width=True):
            with st.spinner("PROCESSING..."):
                try:
                    liens = process_uploaded_file(uploaded_file.getvalue(), state, county or None)
                    st.session_state.liens_data = liens
                    st.session_state.last_fetch_time = date.today()
                    st.sidebar.success(f"✓ LOADED {liens.count} LIENS")
                except Exception as e:
                    st.sidebar.error(f"ERROR: {str(e)}")


def render_filter_controls():
    """Render data filtering controls."""
    st.sidebar.markdown("**FILTERS**")
    liens: LienBatch = st.session_state.liens_data

    # LTV filter
    max_ltv = st.sidebar.slider(
        "MAX LTV %",
        min_value=1.0,
        max_value=100.0,
        value=50.0,
        step=1.0,
    )

    # Face amount filter
    face_amounts = [l.face_amount for l in liens.liens if l.face_amount]
    if face_amounts:
        min_f, max_f = min(face_amounts), max(face_amounts)
        face_range = st.sidebar.slider(
            "FACE AMT $",
            min_value=float(min_f),
            max_value=float(max_f),
            value=(float(min_f), float(max_f)),
            format="$%.0f"
        )
    else:
        face_range = (0.0, 100000.0)

    # County filter
    counties = sorted(set(l.county for l in liens.liens))
    if len(counties) > 1:
        selected_counties = st.sidebar.multiselect(
            "COUNTIES",
            options=counties,
            default=counties
        )
    else:
        selected_counties = counties

    st.session_state.filters = {
        "max_ltv": max_ltv,
        "face_range": face_range,
        "counties": selected_counties
    }


async def scrape_county(county_slug: str) -> List[TaxLien]:
    """Scrape a single county."""
    try:
        adapter = LienHubAdapter(state="FL", county=county_slug, headless=True)
        batch = await adapter.fetch(max_records=200)
        for lien in batch.liens:
            if lien.raw_data is None:
                lien.raw_data = {}
            lien.raw_data["source_url"] = f"https://lienhub.com/county/{county_slug}/countyheld/certificates"
        return batch.liens
    except Exception as e:
        print(f"Error scraping {county_slug}: {e}")
        return []


def scrape_all_counties(state: str) -> LienBatch:
    """Scrape all counties for a state."""
    async def _scrape_all():
        if state == "FL":
            counties = [
                "duval", "hillsborough", "orange", "broward", "miamidade",
                "pinellas", "lee", "brevard", "volusia", "pasco"
            ]
            all_liens = []
            for county in counties:
                liens = await scrape_county(county)
                all_liens.extend(liens)
            return all_liens
        return []

    liens = asyncio.run(_scrape_all())
    return LienBatch(
        liens=liens,
        source_url="https://lienhub.com",
        scrape_timestamp=date.today(),
        state_filter=state,
    )


def process_uploaded_file(file_content: bytes, state: str, county: Optional[str]) -> LienBatch:
    """Process uploaded file."""
    async def _process():
        adapter = FileIngestorAdapter(state=state, county=county, file_content=file_content)
        return await adapter.fetch()
    return asyncio.run(_process())


def lien_batch_to_dataframe(batch: LienBatch) -> pd.DataFrame:
    """Convert LienBatch to DataFrame with source links."""
    records = []
    for lien in batch.liens:
        source_url = lien.raw_data.get("source_url", "") if lien.raw_data else ""
        records.append({
            "COUNTY": lien.county,
            "PARCEL ID": lien.parcel_id,
            "FACE AMT": lien.face_amount,
            "ASSESSED": lien.assessed_value,
            "LTV %": lien.lien_to_value_ratio,
            "TAX YR": lien.raw_data.get("tax_year") if lien.raw_data else None,
            "ISSUED": lien.raw_data.get("issued_date") if lien.raw_data else None,
            "SOURCE": source_url,
        })
    return pd.DataFrame(records)


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply user filters."""
    if "filters" not in st.session_state:
        return df

    f = st.session_state.filters
    filtered = df.copy()

    if "LTV %" in filtered.columns:
        filtered = filtered[
            (filtered["LTV %"].isna()) | (filtered["LTV %"] <= f["max_ltv"])
        ]

    if "FACE AMT" in filtered.columns:
        min_f, max_f = f["face_range"]
        filtered = filtered[
            (filtered["FACE AMT"] >= min_f) & (filtered["FACE AMT"] <= max_f)
        ]

    if "COUNTY" in filtered.columns and "counties" in f:
        filtered = filtered[filtered["COUNTY"].isin(f["counties"])]

    return filtered


def render_main_content():
    """Render main content area."""
    # Header
    st.markdown("# BISHOP LIEN TERMINAL <span class='cursor'>_</span>", unsafe_allow_html=True)

    if st.session_state.liens_data is None:
        render_welcome()
        return

    liens: LienBatch = st.session_state.liens_data

    if liens.count == 0:
        st.warning("NO DATA FOUND. SELECT A STATE OR UPLOAD A FILE.")
        return

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("TOTAL LIENS", f"{liens.count:,}")
    with col2:
        st.metric("FACE VALUE", f"${liens.total_face_amount:,.0f}")
    with col3:
        counties = len(set(l.county for l in liens.liens))
        st.metric("COUNTIES", counties)
    with col4:
        st.metric("UPDATED", st.session_state.last_fetch_time.strftime("%Y-%m-%d"))

    st.markdown("---")

    # Data table
    df = lien_batch_to_dataframe(liens)
    filtered_df = apply_filters(df)

    if len(filtered_df) < len(df):
        st.info(f"DISPLAYING {len(filtered_df)} OF {len(df)} RECORDS")

    # Configure columns
    column_config = {
        "FACE AMT": st.column_config.NumberColumn("FACE AMT", format="$%.2f"),
        "ASSESSED": st.column_config.NumberColumn("ASSESSED", format="$%.2f"),
        "LTV %": st.column_config.NumberColumn("LTV %", format="%.1f%%"),
        "SOURCE": st.column_config.LinkColumn("SOURCE", display_text="VIEW →"),
    }

    st.dataframe(
        filtered_df,
        column_config=column_config,
        use_container_width=True,
        height=500,
        hide_index=True,
    )

    # Export
    st.markdown("")
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        "[ EXPORT CSV ]",
        csv,
        file_name=f"bishop_liens_{date.today().isoformat()}.csv",
        mime="text/csv"
    )


def render_welcome():
    """Welcome screen."""
    st.markdown("""
<div class="welcome-text">

## SYSTEM READY

**BISHOP LIEN TERMINAL** aggregates tax lien certificate data from multiple
county auction platforms into a single unified interface.

---

### COMMANDS

1. Select **SCRAPE STATE** → Choose state → **FETCH ALL LIENS**
2. Data loads from all available counties
3. Filter by LTV, face amount, county
4. Export results to CSV

---

### SUPPORTED SOURCES

| STATE | PLATFORM | STATUS |
|-------|----------|--------|
| FL    | LienHub  | ONLINE |
| IL    | Upload   | READY  |
| AZ    | Pending  | --     |
| NJ    | Pending  | --     |

---

<small>AWAITING INPUT...</small>

</div>
    """, unsafe_allow_html=True)


def main():
    init_session_state()
    render_sidebar()
    render_main_content()


if __name__ == "__main__":
    main()
