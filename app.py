"""
Bishop Lien Terminal
Tax lien data aggregation dashboard
"""

import asyncio
from datetime import date
from typing import Optional, List

import pandas as pd
import streamlit as st

from src.config import STATE_REGISTRY, DEFAULT_METRICS, is_live_scraping_available, get_adapter_for_state
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

# Minimal CSS styling
CUSTOM_CSS = """
<style>
    .main .block-container { max-width: 100%; padding-top: 1rem; }
</style>"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    if "liens_data" not in st.session_state:
        st.session_state.liens_data = None
    if "last_fetch_time" not in st.session_state:
        st.session_state.last_fetch_time = None


def render_sidebar():
    """Render sidebar with instructions and filters."""
    st.sidebar.markdown("### BISHOP LIEN TERMINAL")
    st.sidebar.markdown("---")

    # Instructions - always visible
    st.sidebar.markdown("""
**HOW TO USE**

1. Select a state below
2. Click FETCH to load liens
3. Filter results as needed
4. Export to CSV
    """)

    st.sidebar.markdown("---")

    # Filters (only if data loaded)
    if st.session_state.liens_data is not None and st.session_state.liens_data.count > 0:
        render_filter_controls()
        st.sidebar.markdown("---")

    # Supported sources
    st.sidebar.markdown("""
**DATA SOURCES**

| STATE | STATUS |
|-------|--------|
| FL | <span class='status-online'>ONLINE</span> |
| IL | UPLOAD |
| AZ | -- |
| NJ | -- |
    """, unsafe_allow_html=True)

    # Status footer
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        f"<small style='color: #666;'>VER 1.0.0 | {date.today().strftime('%Y.%m.%d')}</small>",
        unsafe_allow_html=True
    )




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
    """Scrape all counties for a state using the appropriate adapter."""
    async def _scrape():
        adapter = get_adapter_for_state(state, headless=False)
        counties = adapter.get_available_counties()

        # For states with many counties, limit to first 10
        if len(counties) > 10:
            print(f"Limiting to first 10 counties (of {len(counties)} total)")
            counties = counties[:10]

        all_liens = []
        source_url = getattr(adapter, 'base_url', '')

        for county in counties:
            try:
                print(f"  Scraping {county}...")
                county_adapter = get_adapter_for_state(state, county=county, headless=False)
                batch = await county_adapter.fetch(max_records=100)
                if batch.liens:
                    all_liens.extend(batch.liens)
                    print(f"    Found {len(batch.liens)} liens")
                    source_url = batch.source_url or source_url
            except Exception as e:
                print(f"  Error scraping {county}: {e}")
                continue

        return all_liens, source_url

    liens, source_url = asyncio.run(_scrape())
    return LienBatch(
        liens=liens,
        source_url=source_url,
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
    """Welcome screen with prominent action controls."""

    # Center the action area
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("### SELECT DATA SOURCE")
        st.markdown("")

        # Tabs for different modes
        tab1, tab2 = st.tabs(["SCRAPE STATE", "UPLOAD FILE"])

        with tab1:
            st.markdown("")
            state = st.selectbox(
                "STATE",
                options=list(STATE_REGISTRY.keys()),
                format_func=lambda x: f"{x} - {STATE_REGISTRY[x].state_name}" + (" [LIVE]" if is_live_scraping_available(x) else ""),
                label_visibility="collapsed"
            )

            # Show state info and adapter details
            config = STATE_REGISTRY[state]
            is_live = is_live_scraping_available(state)

            if is_live:
                st.success(f"✓ {config.notes}")
            else:
                st.warning(f"⚠ {config.notes}")
                st.caption(f"Platform: {config.primary_adapter.__name__} | Try scraping anyway or use file upload.")

            st.markdown("")

            # Allow scraping attempt for all states
            btn_label = "[ FETCH LIVE DATA ]" if is_live else "[ ATTEMPT SCRAPE ]"
            if st.button(btn_label, type="primary", use_container_width=True, key="fetch_main"):
                with st.spinner(f"SCANNING {state}..."):
                    try:
                        liens = scrape_all_counties(state)
                        if liens:
                            st.session_state.liens_data = liens
                            st.session_state.last_fetch_time = date.today()
                            st.rerun()
                        else:
                            st.warning(f"No data returned. Platform may require registration. Try file upload.")
                    except Exception as e:
                        st.error(f"ERROR: {str(e)}")
                        st.info("This platform likely requires registration. Use UPLOAD FILE tab instead.")

        with tab2:
            st.markdown("")
            upload_state = st.selectbox(
                "STATE",
                options=list(STATE_REGISTRY.keys()),
                format_func=lambda x: f"{x} - {STATE_REGISTRY[x].state_name}",
                key="upload_state_main",
                label_visibility="collapsed"
            )

            county = st.text_input("COUNTY NAME", placeholder="e.g., Cook")

            uploaded_file = st.file_uploader(
                "SELECT FILE",
                type=["csv", "xlsx", "xls"],
                label_visibility="collapsed"
            )

            if uploaded_file:
                if st.button("[ PROCESS FILE ]", type="primary", use_container_width=True, key="process_main"):
                    with st.spinner("PROCESSING..."):
                        try:
                            liens = process_uploaded_file(uploaded_file.getvalue(), upload_state, county or None)
                            st.session_state.liens_data = liens
                            st.session_state.last_fetch_time = date.today()
                            st.rerun()
                        except Exception as e:
                            st.error(f"ERROR: {str(e)}")


def main():
    init_session_state()
    render_sidebar()
    render_main_content()


if __name__ == "__main__":
    main()
