import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
import re
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# ---------------- CONFIG ---------------- #
BASE_URL = "https://extranet.edqm.eu/4DLink1/4DCGI/Query_CEP"
TIMEOUT = 40
MAX_WORKERS = 5

HEADERS = {"User-Agent": "Mozilla/5.0"}

EXPECTED_COLUMNS = [
    "Monograph Number",
    "Substance",
    "Type CEP",
    "Certificate (CEP) Holder",
    "Holder SPOR ORG-ID / SPOR LOC-ID",
    "Certificate (CEP) Number",
    "Issue Date CEP",
    "Status CEP",
    "Renewal due",
    "End date CEP",
    "Closure Date of last Procedure"
]

# ---------------- SESSION ---------------- #
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session

session = create_session()

# ---------------- UI ---------------- #
st.set_page_config(page_title="EDQM CEP Search", page_icon="🔬", layout="wide")
st.title("🔬 EDQM CEP Certification Database Search")

col1, col2 = st.columns([3, 1])

with col1:
    molecules_input = st.text_area(
        "Enter Substance Name or CEP Number (one per line)",
        height=150,
        placeholder="IOHEXOL\n1998-035\nR1-CEP 1998-035 - Rev 03"
    )

with col2:
    cert_type = st.radio("Certificate Type", ["all", "TSE Only", "Herbal Only"])
    search_field = st.selectbox(
        "Search Field",
        ["Substance Name", "Monograph Number", "Holder Name", "CEP Number"]
    )

type_map = {"all": "none", "TSE Only": "tse", "Herbal Only": "herbal"}

# ✅ FIXED mapping (CEP = 4)
field_map = {
    "Substance Name": "1",
    "Monograph Number": "2",
    "Holder Name": "3",
    "CEP Number": "4",
}

# ---------------- SCRAPER ---------------- #
def search_molecule(molecule, case_tse, select_name):

    search_value = molecule.strip()

    # -------- SMART SEARCH TYPE -------- #
    if re.search(r'\d{4}-\d{3}', search_value):
        select_name = "4"  # CEP search

    elif re.search(r'cep', search_value.lower()):
        select_name = "4"  # CEP search

    elif select_name == "1":
        search_value = search_value.upper()

    params = {
        "vSelectName": select_name,
        "Case_TSE": case_tse,
        "vContains": "1",  # enables partial match
        "vtsubName": search_value,
        "SWTP": "1",
        "OK": "Search",
    }

    try:
        resp = session.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        return pd.DataFrame([{"Search Term": molecule, "Error": str(e)}])

    soup = BeautifulSoup(resp.text, "html.parser")

    # -------- FIND TABLE -------- #
    main_table = None
    for table in soup.find_all("table"):
        text = table.get_text(" ", strip=True).lower()
        if "monograph number" in text and "certificate (cep) number" in text:
            main_table = table
            break

    if not main_table:
        return pd.DataFrame([{"Search Term": molecule, "Error": "Table not found"}])

    rows = main_table.find_all("tr")

    # -------- EXTRACT DATA -------- #
    data_rows = []
    start = False

    for row in rows:
        cells = row.find_all("td")

        if cells and re.match(r'^\d+', cells[0].get_text(strip=True)):
            start = True

        if not start:
            continue

        if not cells:
            continue

        row_data = []
        for cell in cells:
            text = cell.get_text(" ", strip=True)
            colspan = int(cell.get("colspan", 1))
            row_data.extend([text] * colspan)

        if row_data and re.match(r'^\d+', row_data[0]):
            data_rows.append(row_data)

    if not data_rows:
        return pd.DataFrame([{
            "Search Term": molecule,
            "Error": "No valid records found"
        }])

    # -------- STRUCTURE -------- #
    clean_rows = []

    for r in data_rows:
        if len(r) >= len(EXPECTED_COLUMNS):
            clean_rows.append(r[:len(EXPECTED_COLUMNS)])

    if not clean_rows:
        return pd.DataFrame([{
            "Search Term": molecule,
            "Error": "No structured data found"
        }])

    df = pd.DataFrame(clean_rows, columns=EXPECTED_COLUMNS)

    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df.insert(0, "Search Term", molecule.upper())

    return df


# ---------------- EXPORT ---------------- #
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
    return output.getvalue()


# ---------------- SEARCH ---------------- #
if st.button("🔍 Search", use_container_width=True):

    molecules = [m.strip() for m in molecules_input.splitlines() if m.strip()]

    if not molecules:
        st.warning("Please enter at least one value")
        st.stop()

    progress = st.progress(0)
    results = []

    def process(mol):
        time.sleep(random.uniform(0.3, 0.8))
        return search_molecule(mol, type_map[cert_type], field_map[search_field])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process, m) for m in molecules]

        for i, future in enumerate(futures):
            df = future.result()

            if "Error" in df.columns:
                st.warning(f"{df.iloc[0]['Search Term']}: {df.iloc[0]['Error']}")
            else:
                results.append(df)

            progress.progress((i + 1) / len(molecules))

    progress.empty()

    if results:
        final_df = pd.concat(results, ignore_index=True)
        st.session_state["results"] = final_df
        st.success(f"Found {len(final_df)} records")
    else:
        st.error("No results found")


# ---------------- DISPLAY ---------------- #
if "results" in st.session_state:
    df = st.session_state["results"]

    st.subheader(f"Results ({len(df)} rows)")

    if "Status CEP" in df.columns:
        status_filter = st.selectbox(
            "Filter by Status",
            ["All"] + sorted(df["Status CEP"].dropna().astype(str).unique().tolist())
        )

        if status_filter != "All":
            df = df[df["Status CEP"] == status_filter]

    st.dataframe(df, use_container_width=True, height=600)

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            "📥 Download Excel",
            to_excel(df),
            "EDQM_CEP_Results.xlsx"
        )

    with col2:
        st.download_button(
            "📄 Download CSV",
            df.to_csv(index=False),
            "EDQM_CEP_Results.csv"
        )