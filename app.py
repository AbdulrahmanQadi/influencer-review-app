import os
import random
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# =========================================================
# Config
# =========================================================
QUEUE_PATH = "data/review_queue.csv"
DECISIONS_PATH = "data/review_decisions.csv"

REVIEW_LABELS = ["belongs", "does_not_belong", "unsure"]
REVIEW_LABELS_DISPLAY = {
    "belongs": "Belongs in cluster",
    "does_not_belong": "Does not belong",
    "unsure": "Unsure",
}

REVIEW_OUTCOMES = [
    "accepted_current_cluster",
    "remove_from_cluster",
    "add_to_cluster",
    "unclear",
]
REASON_CATEGORIES = [
    "creator_individual_or_creator_brand",
    "business_or_company",
    "agency_or_network",
    "publisher_or_media",
    "utility_or_non_creator",
    "insufficient_information",
    "other",
]

OUTCOME_LABELS = {
    "accepted_current_cluster": "Keep in cluster",
    "remove_from_cluster": "Remove from cluster",
    "add_to_cluster": "Add to cluster",
    "unclear": "Needs follow-up",
}

REASON_LABELS = {
    "creator_individual_or_creator_brand": "Creator-led publisher",
    "business_or_company": "Business or company",
    "agency_or_network": "Agency or network",
    "publisher_or_media": "Publisher or media",
    "utility_or_non_creator": "Utility / non-creator",
    "insufficient_information": "Not enough information",
    "other": "Other",
}

REQUIRED_QUEUE_COLUMNS = [
    "review_batch_id",
    "PublisherKey",
    "Publisher",
    "PublisherWebSite",
    "PublisherDescription",
    "current_publisher_vertical",
    "current_publisher_subvertical",
    "current_publisher_type_group",
    "current_publisher_group",
    "priority_bucket",
    "candidate_reason",
    "signal_possible_business_entity",
    "signal_network_or_agency",
]

DECISION_COLUMNS = [
    "review_batch_id",
    "PublisherKey",
    "reviewed_cluster_label",
    "review_outcome",
    "review_reason_category",
    "review_comment",
    "reviewer_name",
    "reviewed_at",
]

BUCKET_DEFINITIONS = {
    "p1_current_cluster_strong": {
        "label": "Current cluster strong",
        "summary": "Already in the current Influencer / Content Creator cluster and also has strong creator/social signals.",
        "reviewer_focus": "Usually likely to belong. Confirm that it is genuinely creator-led and not a false positive.",
        "chip_class": "chip-green",
    },
    "p2_current_cluster": {
        "label": "Current cluster",
        "summary": "Currently sits in the cluster, but the supporting creator/social evidence is weaker.",
        "reviewer_focus": "Check whether it truly belongs or should be removed from the cluster.",
        "chip_class": "chip-blue",
    },
    "p3_hidden_positive_strong": {
        "label": "Hidden positive strong",
        "summary": "Not clearly in the current cluster, but has strong creator/social evidence and looks like a likely missing creator.",
        "reviewer_focus": "Check whether this should be added to the cluster.",
        "chip_class": "chip-purple",
    },
    "p4_hidden_positive": {
        "label": "Hidden positive",
        "summary": "May be a creator-led publisher, but the evidence is broader or noisier.",
        "reviewer_focus": "Check carefully. This bucket can contain both genuine creators and noisy false positives.",
        "chip_class": "chip-amber",
    },
    "p5_adjacent_supported": {
        "label": "Adjacent supported",
        "summary": "Comes from an adjacent content area and has some supporting creator/social signals.",
        "reviewer_focus": "Decide whether this is truly creator-led or better treated as adjacent content/media.",
        "chip_class": "chip-slate",
    },
    "p6_social_and_keyword": {
        "label": "Social + keyword",
        "summary": "Has both social-domain and description-keyword signals, but sits outside the stronger priority groups.",
        "reviewer_focus": "Useful boundary case. Decide whether it genuinely belongs or is just loosely related.",
        "chip_class": "chip-red",
    },
}

# =========================================================
# Page setup
# =========================================================
st.set_page_config(
    page_title="Influencer Review",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =========================================================
# Styling
# =========================================================
st.markdown(
    """
    <style>
    .block-container {
        max-width: 1560px;
        padding-top: 3rem;
        padding-bottom: 1.2rem;
    }

    .panel-heading {
        font-size: 1.05rem;
        font-weight: 800;
        color: #F8FBFF;
        line-height: 1.2;
        margin: 0 0 10px 0;
        letter-spacing: -0.01em;
    }

    .stApp {
        background:
            radial-gradient(circle at top left, rgba(91,140,255,0.10), transparent 26%),
            radial-gradient(circle at top right, rgba(162,89,255,0.10), transparent 22%),
            linear-gradient(180deg, #07111F 0%, #091525 100%);
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0A1322 0%, #0C182A 100%);
        border-right: 1px solid rgba(255,255,255,0.06);
    }

    .hero {
        position: relative;
        overflow: hidden;
        background: linear-gradient(135deg, rgba(22,35,60,0.92) 0%, rgba(15,25,44,0.96) 100%);
        border: 1px solid rgba(126,167,255,0.18);
        border-radius: 28px;
        padding: 22px 24px;
        margin-bottom: 10px;
        box-shadow:
            0 10px 40px rgba(0,0,0,0.28),
            inset 0 1px 0 rgba(255,255,255,0.04);
    }

    .hero:before {
        content: "";
        position: absolute;
        top: -40px;
        right: -20px;
        width: 220px;
        height: 220px;
        background: radial-gradient(circle, rgba(91,140,255,0.20) 0%, transparent 70%);
        pointer-events: none;
    }

    .hero:after {
        content: "";
        position: absolute;
        bottom: -50px;
        left: -40px;
        width: 240px;
        height: 240px;
        background: radial-gradient(circle, rgba(162,89,255,0.16) 0%, transparent 70%);
        pointer-events: none;
    }

    .hero h1 {
        margin: 0 0 8px 0;
        font-size: 2.1rem;
        line-height: 1.08;
        color: #F8FBFF;
        font-weight: 800;
        letter-spacing: -0.02em;
    }

    .hero p {
        margin: 0;
        color: #C7D4E8;
        font-size: 0.98rem;
        line-height: 1.5;
        max-width: 840px;
    }

    .subtle-card {
        background: rgba(12,22,38,0.74);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 18px;
        padding: 12px 14px;
        box-shadow: 0 10px 24px rgba(0,0,0,0.18);
    }

    .subtle-title {
        font-size: 0.78rem;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        color: #8FA6C6;
        margin-bottom: 4px;
    }

    .subtle-value {
        font-size: 1.14rem;
        font-weight: 800;
        color: #F8FBFF;
    }

    .publisher-title {
        font-size: 2.1rem;
        font-weight: 800;
        color: #F8FBFF;
        line-height: 1.08;
        margin-bottom: 8px;
        letter-spacing: -0.02em;
    }

    .publisher-link {
        font-size: 1rem;
        margin-bottom: 14px;
        color: #C9D6EA;
    }

    .publisher-link a {
        color: #8CB4FF !important;
        text-decoration: none;
        font-weight: 600;
    }

    .publisher-link a:hover {
        text-decoration: underline;
    }

    .chip-row {
        margin-bottom: 10px;
    }

    .chip {
        display: inline-block;
        padding: 7px 12px;
        border-radius: 999px;
        font-size: 0.81rem;
        font-weight: 700;
        margin-right: 8px;
        margin-bottom: 8px;
        border: 1px solid transparent;
        letter-spacing: 0.01em;
    }

    .chip-blue {
        background: rgba(59,130,246,0.16);
        color: #BFD7FF;
        border-color: rgba(96,165,250,0.28);
    }

    .chip-green {
        background: rgba(16,185,129,0.16);
        color: #BBF7D0;
        border-color: rgba(74,222,128,0.24);
    }

    .chip-amber {
        background: rgba(245,158,11,0.16);
        color: #FCD9A6;
        border-color: rgba(251,191,36,0.22);
    }

    .chip-red {
        background: rgba(239,68,68,0.16);
        color: #FECACA;
        border-color: rgba(248,113,113,0.22);
    }

    .chip-slate {
        background: rgba(148,163,184,0.14);
        color: #D7E0EC;
        border-color: rgba(203,213,225,0.14);
    }

    .chip-purple {
        background: rgba(168,85,247,0.16);
        color: #E9D5FF;
        border-color: rgba(196,181,253,0.22);
    }

    .status-bar {
        padding: 11px 13px;
        border-radius: 14px;
        font-size: 0.92rem;
        font-weight: 600;
        margin-bottom: 14px;
    }

    .status-unreviewed {
        background: linear-gradient(135deg, rgba(37,99,235,0.18) 0%, rgba(59,130,246,0.10) 100%);
        color: #BFD7FF;
        border: 1px solid rgba(96,165,250,0.22);
    }

    .status-reviewed {
        background: linear-gradient(135deg, rgba(16,185,129,0.18) 0%, rgba(34,197,94,0.10) 100%);
        color: #BBF7D0;
        border: 1px solid rgba(74,222,128,0.22);
    }

    .status-locked {
        background: linear-gradient(135deg, rgba(245,158,11,0.18) 0%, rgba(249,115,22,0.10) 100%);
        color: #FCD9A6;
        border: 1px solid rgba(251,191,36,0.22);
    }

    .explanation {
        background: linear-gradient(135deg, rgba(15,28,48,0.82) 0%, rgba(12,23,40,0.82) 100%);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 18px;
        padding: 14px 16px;
        margin: 10px 0 16px 0;
    }

    .explanation-kicker {
        font-size: 0.75rem;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #86A3C8;
        margin-bottom: 6px;
    }

    .explanation-title {
        font-size: 1rem;
        font-weight: 700;
        color: #F8FBFF;
        margin-bottom: 6px;
    }

    .explanation-text {
        font-size: 0.95rem;
        line-height: 1.55;
        color: #CFD8E6;
        margin-bottom: 8px;
    }

    .explanation-focus {
        font-size: 0.92rem;
        line-height: 1.5;
        color: #B4C3D9;
    }

    .description-box {
        background: linear-gradient(135deg, rgba(14,26,44,0.92) 0%, rgba(12,22,37,0.92) 100%);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 18px;
        padding: 15px 16px;
        color: #F5F7FB;
        font-size: 1rem;
        line-height: 1.65;
        margin-bottom: 16px;
    }

    .field-label {
        font-size: 0.78rem;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        color: #A6B8D3;
        margin-bottom: 4px;
    }

    .field-value {
        font-size: 1rem;
        color: #F5F7FB;
        margin-bottom: 12px;
        word-break: break-word;
    }

    .review-hint {
        background: linear-gradient(135deg, rgba(22,35,60,0.9) 0%, rgba(15,24,40,0.9) 100%);
        border: 1px solid rgba(255,255,255,0.07);
        color: #C8D4E7;
        border-radius: 14px;
        padding: 11px 13px;
        font-size: 0.92rem;
        margin-bottom: 12px;
        line-height: 1.5;
    }

    .legend-card {
        background: rgba(12,22,38,0.72);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 16px;
        padding: 12px 14px;
        margin-bottom: 10px;
    }

    .legend-title {
        font-size: 0.96rem;
        font-weight: 700;
        color: #F5F7FB;
        margin-bottom: 4px;
    }

    .legend-text {
        font-size: 0.9rem;
        color: #C8D4E7;
        line-height: 1.45;
        margin-bottom: 6px;
    }

    .legend-focus {
        font-size: 0.88rem;
        color: #9FB2CC;
        line-height: 1.45;
    }

    div[data-testid="stMetric"] {
        background: rgba(12,22,38,0.72);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 18px;
        padding: 10px 14px;
    }

    div[data-testid="stMetric"] label {
        color: #8FA6C6 !important;
    }

    div[data-testid="stMetricValue"] {
        color: #F8FBFF !important;
    }

    div.stButton > button {
        border-radius: 14px !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        background: linear-gradient(135deg, rgba(18,31,52,0.96) 0%, rgba(14,24,40,0.96) 100%) !important;
        color: #F5F7FB !important;
        font-weight: 700 !important;
        transition: all 0.15s ease-in-out !important;
    }

    div.stButton > button:hover {
        border-color: rgba(91,140,255,0.30) !important;
        box-shadow: 0 0 0 1px rgba(91,140,255,0.08), 0 0 24px rgba(91,140,255,0.10);
        transform: translateY(-1px);
    }

    div.stDownloadButton > button {
        border-radius: 14px !important;
        font-weight: 700 !important;
    }

    div[data-baseweb="input"] > div,
    div[data-baseweb="select"] > div,
    textarea {
        border-radius: 14px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# Helpers
# =========================================================
def ensure_storage():
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(DECISIONS_PATH):
        pd.DataFrame(columns=DECISION_COLUMNS).to_csv(DECISIONS_PATH, index=False)


def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x)


def boolish(x):
    return safe_str(x).strip().lower() in {"true", "1", "yes", "y"}


def normalise_url(url: str) -> str:
    url = safe_str(url).strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def extract_domain(url: str) -> str:
    url = normalise_url(url)
    if not url:
        return ""
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


def infer_is_current_cluster(row: pd.Series) -> bool:
    subvertical = safe_str(row.get("current_publisher_subvertical", "")).lower()
    type_group = safe_str(row.get("current_publisher_type_group", "")).lower()
    group = safe_str(row.get("current_publisher_group", "")).lower()
    return (
        "content creators" in subvertical
        or "influencer" in subvertical
        or type_group == "social content"
        or "influencer" in group
    )


def default_outcome_for_label(label: str, row: pd.Series) -> str:
    current_cluster = infer_is_current_cluster(row)
    if label == "belongs":
        return "accepted_current_cluster" if current_cluster else "add_to_cluster"
    if label == "does_not_belong":
        return "remove_from_cluster" if current_cluster else "unclear"
    return "unclear"


def default_reason_for_label(label: str) -> str:
    if label == "belongs":
        return "creator_individual_or_creator_brand"
    if label == "does_not_belong":
        return "business_or_company"
    return "insufficient_information"


def get_bucket_meta(bucket: str):
    return BUCKET_DEFINITIONS.get(
        bucket,
        {
            "label": bucket or "Unspecified bucket",
            "summary": "No description available.",
            "reviewer_focus": "Review normally.",
            "chip_class": "chip-slate",
        },
    )


def render_bucket_explanation(bucket: str):
    meta = get_bucket_meta(bucket)
    return f"""
    <div class="explanation">
        <div class="explanation-kicker">Why this publisher is in the review queue</div>
        <div class="explanation-title">{meta["label"]}</div>
        <div class="explanation-text">{meta["summary"]}</div>
        <div class="explanation-focus"><strong>What to check:</strong> {meta["reviewer_focus"]}</div>
    </div>
    """


@st.cache_data
def load_queue():
    if not os.path.exists(QUEUE_PATH):
        return None

    df = pd.read_csv(QUEUE_PATH, dtype=str).fillna("")
    for col in REQUIRED_QUEUE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df["PublisherKey"] = df["PublisherKey"].astype(str)
    df["review_batch_id"] = df["review_batch_id"].astype(str)
    return df


def load_decisions():
    if not os.path.exists(DECISIONS_PATH):
        return pd.DataFrame(columns=DECISION_COLUMNS)
    df = pd.read_csv(DECISIONS_PATH, dtype=str).fillna("")
    if "PublisherKey" in df.columns:
        df["PublisherKey"] = df["PublisherKey"].astype(str)
    if "review_batch_id" in df.columns:
        df["review_batch_id"] = df["review_batch_id"].astype(str)
    return df


def get_existing_decision_for_row(publisher_key: str, review_batch_id: str, decisions: pd.DataFrame):
    if decisions.empty:
        return None

    match = decisions[
        (decisions["PublisherKey"].astype(str) == str(publisher_key))
        & (decisions["review_batch_id"].astype(str) == str(review_batch_id))
    ]
    if match.empty:
        return None
    match = match.sort_values("reviewed_at")
    return match.iloc[-1].to_dict()


def can_current_reviewer_edit(existing_decision, reviewer_name: str) -> bool:
    if existing_decision is None:
        return True
    existing_reviewer = safe_str(existing_decision.get("reviewer_name", "")).strip()
    current = reviewer_name.strip()
    if not existing_reviewer:
        return True
    if not current:
        return False
    return existing_reviewer == current


def save_or_update_decision(decision_row: dict):
    decisions = load_decisions()

    publisher_key = str(decision_row["PublisherKey"])
    review_batch_id = str(decision_row["review_batch_id"])
    current_reviewer = str(decision_row["reviewer_name"]).strip()

    existing = get_existing_decision_for_row(publisher_key, review_batch_id, decisions)
    if existing is not None:
        existing_reviewer = safe_str(existing.get("reviewer_name", "")).strip()
        if existing_reviewer and existing_reviewer != current_reviewer:
            return False, f"Already reviewed by {existing_reviewer}. This row is read-only for other reviewers."

        mask = (
            (decisions["PublisherKey"].astype(str) == publisher_key)
            & (decisions["review_batch_id"].astype(str) == review_batch_id)
        )
        decisions = decisions.loc[~mask].copy()

    decisions = pd.concat([decisions, pd.DataFrame([decision_row])], ignore_index=True)
    decisions.to_csv(DECISIONS_PATH, index=False)
    return True, "Saved."


def merge_review_status(queue: pd.DataFrame, decisions: pd.DataFrame) -> pd.DataFrame:
    q = queue.copy()

    if decisions.empty:
        q["is_reviewed"] = False
        q["reviewed_cluster_label"] = ""
        q["review_outcome"] = ""
        q["review_reason_category"] = ""
        q["reviewer_name"] = ""
        q["reviewed_at"] = ""
        return q

    latest = decisions.sort_values("reviewed_at").drop_duplicates(
        subset=["PublisherKey", "review_batch_id"], keep="last"
    )

    merged = q.merge(
        latest[
            [
                "PublisherKey",
                "review_batch_id",
                "reviewed_cluster_label",
                "review_outcome",
                "review_reason_category",
                "reviewer_name",
                "reviewed_at",
            ]
        ],
        on=["PublisherKey", "review_batch_id"],
        how="left",
    ).fillna("")

    merged["is_reviewed"] = merged["reviewed_cluster_label"] != ""
    return merged


def render_chips(row: pd.Series):
    parts = []

    bucket = safe_str(row.get("priority_bucket", ""))
    meta = get_bucket_meta(bucket)
    parts.append(f'<span class="chip {meta["chip_class"]}">{meta["label"]}</span>')

    if infer_is_current_cluster(row):
        parts.append('<span class="chip chip-blue">currently in cluster</span>')
    else:
        parts.append('<span class="chip chip-amber">not currently in cluster</span>')

    if boolish(row.get("signal_possible_business_entity", "")):
        parts.append('<span class="chip chip-red">possible business/entity</span>')

    if boolish(row.get("signal_network_or_agency", "")):
        parts.append('<span class="chip chip-red">possible network/agency</span>')

    return "".join(parts)


# =========================================================
# Boot
# =========================================================
ensure_storage()

queue = load_queue()
if queue is None:
    st.error("Could not find `data/review_queue.csv`.")
    st.info("Export your Databricks review export table and save it locally as `data/review_queue.csv`.")
    st.stop()

decisions = load_decisions()
queue_merged = merge_review_status(queue, decisions)
review_queue = queue_merged[queue_merged["is_reviewed"] == False].reset_index(drop=True)

if "current_idx" not in st.session_state:
    st.session_state.current_idx = 0
if "selected_publisher_key" not in st.session_state:
    st.session_state.selected_publisher_key = None
if "reviewer_name" not in st.session_state:
    st.session_state.reviewer_name = ""

# =========================================================
# Sidebar
# =========================================================
with st.sidebar:
    st.markdown("## Review progress")

    total_rows = len(queue_merged)
    reviewed_rows = int(queue_merged["is_reviewed"].sum())
    remaining_rows = total_rows - reviewed_rows
    pct = reviewed_rows / total_rows if total_rows else 0

    st.metric("Total rows", total_rows)
    st.metric("Reviewed", reviewed_rows)
    st.metric("Remaining", remaining_rows)
    st.progress(pct)
    st.caption(f"{pct:.1%} complete")

    st.markdown("---")
    st.markdown("## Export")
    if os.path.exists(DECISIONS_PATH):
        with open(DECISIONS_PATH, "rb") as f:
            st.download_button(
                "Download decisions CSV",
                data=f,
                file_name="review_decisions.csv",
                mime="text/csv",
                use_container_width=True,
            )

# =========================================================
# Determine current row
# =========================================================
selected_override_key = st.session_state.selected_publisher_key
row = None
is_override_mode = False

if selected_override_key:
    match = queue_merged[queue_merged["PublisherKey"].astype(str) == str(selected_override_key)]
    if not match.empty:
        row = match.iloc[0]
        is_override_mode = True

if row is None:
    if review_queue.empty:
        st.markdown(
            """
            <div class="hero">
                <h1>All publishers in this batch have been reviewed</h1>
                <p>You can still browse reviewed rows in the Queue tab and download the decisions CSV from the sidebar.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.stop()

    st.session_state.current_idx = min(st.session_state.current_idx, len(review_queue) - 1)
    row = review_queue.iloc[st.session_state.current_idx]
    is_override_mode = False

publisher_key = safe_str(row["PublisherKey"])
review_batch_id = safe_str(row.get("review_batch_id", "review_batch_001"))
existing_decision = get_existing_decision_for_row(publisher_key, review_batch_id, decisions)
row_locked_by_other = existing_decision is not None and not can_current_reviewer_edit(existing_decision, st.session_state.reviewer_name)

position_in_queue = (
    st.session_state.current_idx + 1 if not is_override_mode and len(review_queue) > 0 else "-"
)

current_reviewer = st.session_state.reviewer_name.strip()
reviewer_count = 0
if current_reviewer and not decisions.empty:
    reviewer_count = int(
        (decisions["reviewer_name"].fillna("").str.strip() == current_reviewer).sum()
    )

# =========================================================
# Header
# =========================================================
top_left, top_right = st.columns([4.2, 1.5], gap="large")

with top_left:
    st.markdown(
        """
        <div class="hero">
            <h1>Influencer / Content Creator Review</h1>
            <p>Review one publisher at a time, make a clear decision, and help build the first business benchmark for this segment.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with top_right:
    with st.container(border=True):
        st.markdown(
            '<div class="panel-heading">Reviewer console</div>',
            unsafe_allow_html=True,
        )
        reviewer_name_input = st.text_input(
            "Your name",
            value=st.session_state.reviewer_name,
            label_visibility="collapsed",
            placeholder="Enter your name before reviewing",
        )
        st.session_state.reviewer_name = reviewer_name_input.strip()
        st.caption("Reviewer name is required before saving any decision.")

# =========================================================
# Status cards
# =========================================================
status_c1, status_c2, status_c3 = st.columns([1.2, 1.2, 1.6], gap="small")

with status_c1:
    st.markdown(
        f"""
        <div class="subtle-card" style="margin-bottom:10px;">
            <div class="subtle-title">Queue position</div>
            <div class="subtle-value">{position_in_queue} / {len(review_queue) if len(review_queue) > 0 else total_rows}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with status_c2:
    st.markdown(
        f"""
        <div class="subtle-card" style="margin-bottom:10px;">
            <div class="subtle-title">Publisher key</div>
            <div class="subtle-value">{publisher_key}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with status_c3:
    st.markdown(
        f"""
        <div class="subtle-card" style="margin-bottom:10px;">
            <div class="subtle-title">Overall progress</div>
            <div class="subtle-value">{reviewed_rows} reviewed · {remaining_rows} remaining</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# =========================================================
# Navigation
# =========================================================
nav1, nav2, nav3, nav4, nav5 = st.columns([1, 1, 1, 1, 3])

if not is_override_mode:
    with nav1:
        if st.button("⏮ First", use_container_width=True):
            st.session_state.current_idx = 0
            st.rerun()

    with nav2:
        if st.button("◀ Previous", use_container_width=True):
            st.session_state.current_idx = max(0, st.session_state.current_idx - 1)
            st.rerun()

    with nav3:
        if st.button("Next ▶", use_container_width=True):
            st.session_state.current_idx = min(len(review_queue) - 1, st.session_state.current_idx + 1)
            st.rerun()

    with nav4:
        if st.button("🎲 Random", use_container_width=True):
            st.session_state.current_idx = random.randint(0, len(review_queue) - 1)
            st.rerun()
else:
    with nav1:
        if st.button("Back to unreviewed queue", use_container_width=True):
            st.session_state.selected_publisher_key = None
            st.rerun()

# =========================================================
# Main workspace
# =========================================================
left, right = st.columns([1.75, 1.05], gap="medium")

with left:
    with st.container(border=True):
        if row_locked_by_other:
            st.markdown(
                f'<div class="status-bar status-locked">Already reviewed by {safe_str(existing_decision.get("reviewer_name", ""))}. This row is read-only for other reviewers.</div>',
                unsafe_allow_html=True,
            )
        elif existing_decision:
            st.markdown(
                f'<div class="status-bar status-reviewed">Reviewed by you ({safe_str(existing_decision.get("reviewer_name", ""))}). You can still edit this decision.</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="status-bar status-unreviewed">Not reviewed yet</div>',
                unsafe_allow_html=True,
            )

        website = normalise_url(safe_str(row.get("PublisherWebSite", "")))
        domain = extract_domain(website)

        st.markdown(
            f'<div class="publisher-title">{safe_str(row.get("Publisher", "Unknown publisher"))}</div>',
            unsafe_allow_html=True,
        )

        if website:
            st.markdown(
                f'<div class="publisher-link"><strong>Website:</strong> <a href="{website}" target="_blank">{domain or website}</a></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="publisher-link"><strong>Website:</strong> No website provided</div>',
                unsafe_allow_html=True,
            )

        st.markdown(f'<div class="chip-row">{render_chips(row)}</div>', unsafe_allow_html=True)
        st.markdown(render_bucket_explanation(safe_str(row.get("priority_bucket", ""))), unsafe_allow_html=True)

        st.markdown("### Description")
        description = safe_str(row.get("PublisherDescription", ""))
        st.markdown(
            f'<div class="description-box">{description if description else "No description provided."}</div>',
            unsafe_allow_html=True,
        )

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### Current classification")
            st.markdown('<div class="field-label">Vertical</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="field-value">{safe_str(row.get("current_publisher_vertical", "")) or "Blank"}</div>',
                unsafe_allow_html=True,
            )
            st.markdown('<div class="field-label">Subvertical</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="field-value">{safe_str(row.get("current_publisher_subvertical", "")) or "Blank"}</div>',
                unsafe_allow_html=True,
            )

        with c2:
            st.markdown("### Current grouping")
            st.markdown('<div class="field-label">Type group</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="field-value">{safe_str(row.get("current_publisher_type_group", "")) or "Blank"}</div>',
                unsafe_allow_html=True,
            )
            st.markdown('<div class="field-label">Group</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="field-value">{safe_str(row.get("current_publisher_group", "")) or "Blank"}</div>',
                unsafe_allow_html=True,
            )

        with st.expander("Show review context and technical signals"):
            context_df = pd.DataFrame(
                {
                    "field": [
                        "review_batch_id",
                        "PublisherKey",
                        "priority_bucket",
                        "candidate_reason",
                        "signal_possible_business_entity",
                        "signal_network_or_agency",
                    ],
                    "value": [
                        safe_str(row.get("review_batch_id", "")),
                        safe_str(row.get("PublisherKey", "")),
                        safe_str(row.get("priority_bucket", "")),
                        safe_str(row.get("candidate_reason", "")),
                        safe_str(row.get("signal_possible_business_entity", "")),
                        safe_str(row.get("signal_network_or_agency", "")),
                    ],
                }
            )
            st.dataframe(context_df, use_container_width=True, hide_index=True)

with right:
    with st.container(border=True):
        st.markdown("### Review decision")
        st.markdown(
            '<div class="review-hint">Make the simplest confident decision you can. Use <strong>Unsure</strong> only when the available information genuinely is not enough.</div>',
            unsafe_allow_html=True,
        )

        default_label = existing_decision["reviewed_cluster_label"] if existing_decision else "belongs"
        default_outcome = existing_decision["review_outcome"] if existing_decision else default_outcome_for_label(default_label, row)
        default_reason = existing_decision["review_reason_category"] if existing_decision else default_reason_for_label(default_label)
        default_comment = existing_decision["review_comment"] if existing_decision else ""

        with st.form(key=f"review_form_{publisher_key}"):
            reviewed_cluster_label = st.radio(
                "Does this publisher belong in the Influencer / Content Creator cluster?",
                REVIEW_LABELS,
                index=REVIEW_LABELS.index(default_label) if default_label in REVIEW_LABELS else 0,
                horizontal=True,
                format_func=lambda x: REVIEW_LABELS_DISPLAY.get(x, x),
                disabled=row_locked_by_other,
            )

            suggested_outcome = default_outcome_for_label(reviewed_cluster_label, row)
            st.caption(f"Suggested outcome: {OUTCOME_LABELS.get(suggested_outcome, suggested_outcome)}")

            review_outcome = st.selectbox(
                "Review outcome",
                REVIEW_OUTCOMES,
                index=REVIEW_OUTCOMES.index(default_outcome) if default_outcome in REVIEW_OUTCOMES else 0,
                format_func=lambda x: OUTCOME_LABELS.get(x, x),
                disabled=row_locked_by_other,
            )

            review_reason_category = st.selectbox(
                "Reason category",
                REASON_CATEGORIES,
                index=REASON_CATEGORIES.index(default_reason) if default_reason in REASON_CATEGORIES else 0,
                format_func=lambda x: REASON_LABELS.get(x, x),
                disabled=row_locked_by_other,
            )

            review_comment = st.text_area(
                "Comment",
                value=default_comment,
                height=120,
                placeholder="Optional note for ambiguous, misleading, or interesting cases...",
                disabled=row_locked_by_other,
            )

            b1, b2, b3 = st.columns(3)
            save_next = b1.form_submit_button("Save & Next", use_container_width=True, disabled=row_locked_by_other)
            save = b2.form_submit_button("Save", use_container_width=True, disabled=row_locked_by_other)
            skip = b3.form_submit_button("Skip", use_container_width=True)

            if save or save_next:
                if not st.session_state.reviewer_name.strip():
                    st.error("Please enter your name in the Reviewer console before saving.")
                else:
                    ok, msg = save_or_update_decision(
                        {
                            "review_batch_id": review_batch_id,
                            "PublisherKey": publisher_key,
                            "reviewed_cluster_label": reviewed_cluster_label,
                            "review_outcome": review_outcome,
                            "review_reason_category": review_reason_category,
                            "review_comment": review_comment,
                            "reviewer_name": st.session_state.reviewer_name.strip(),
                            "reviewed_at": datetime.utcnow().isoformat(),
                        }
                    )
                    if ok:
                        if save_next:
                            st.session_state.selected_publisher_key = None
                            st.session_state.current_idx = min(len(review_queue) - 1, st.session_state.current_idx + 1)
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            if skip:
                st.session_state.selected_publisher_key = None
                if not is_override_mode:
                    st.session_state.current_idx = min(len(review_queue) - 1, st.session_state.current_idx + 1)
                st.rerun()

        if existing_decision:
            st.markdown("---")
            st.markdown("**Saved decision**")
            st.write(f"Label: {REVIEW_LABELS_DISPLAY.get(safe_str(existing_decision.get('reviewed_cluster_label', '')), safe_str(existing_decision.get('reviewed_cluster_label', '')))}")
            st.write(f"Outcome: {OUTCOME_LABELS.get(safe_str(existing_decision.get('review_outcome', '')), safe_str(existing_decision.get('review_outcome', '')))}")
            st.write(f"Reason: {REASON_LABELS.get(safe_str(existing_decision.get('review_reason_category', '')), safe_str(existing_decision.get('review_reason_category', '')))}")
            st.write(f"Reviewer: {safe_str(existing_decision.get('reviewer_name', ''))}")
            st.write(f"Reviewed at: {safe_str(existing_decision.get('reviewed_at', ''))}")

# =========================================================
# Tabs
# =========================================================
tab_queue, tab_analytics, tab_help = st.tabs(["Queue", "Analytics", "Help"])

with tab_queue:
    with st.container(border=True):
        st.markdown("### Queue browser")

        search_q = st.text_input("Find a reviewed or unreviewed publisher")
        queue_display = queue_merged.copy()

        if search_q.strip():
            s = search_q.strip().lower()
            queue_display = queue_display[
                queue_display["Publisher"].str.lower().str.contains(s, na=False)
                | queue_display["PublisherKey"].astype(str).str.lower().str.contains(s, na=False)
                | queue_display["PublisherWebSite"].str.lower().str.contains(s, na=False)
            ].copy()

        if not queue_display.empty:
            queue_display_view = queue_display[
                [
                    "PublisherKey",
                    "Publisher",
                    "priority_bucket",
                    "current_publisher_type_group",
                    "current_publisher_subvertical",
                    "is_reviewed",
                    "reviewed_cluster_label",
                    "review_outcome",
                    "reviewer_name",
                ]
            ].rename(
                columns={
                    "current_publisher_type_group": "type_group",
                    "current_publisher_subvertical": "subvertical",
                }
            ).copy()

            queue_display_view["priority_bucket"] = queue_display_view["priority_bucket"].apply(
                lambda x: get_bucket_meta(safe_str(x))["label"]
            )
            queue_display_view["reviewed_cluster_label"] = queue_display_view["reviewed_cluster_label"].apply(
                lambda x: REVIEW_LABELS_DISPLAY.get(safe_str(x), safe_str(x))
            )
            queue_display_view["review_outcome"] = queue_display_view["review_outcome"].apply(
                lambda x: OUTCOME_LABELS.get(safe_str(x), safe_str(x))
            )

            st.dataframe(queue_display_view, use_container_width=True, height=420)

            jump_options = queue_display[["PublisherKey", "Publisher"]].copy()
            jump_options["label"] = jump_options["Publisher"] + " (" + jump_options["PublisherKey"].astype(str) + ")"

            selected_label = st.selectbox(
                "Open a specific publisher in the review panel",
                options=jump_options["label"].tolist(),
            )

            if st.button("Open selected publisher", use_container_width=True):
                selected_row = jump_options[jump_options["label"] == selected_label].iloc[0]
                st.session_state.selected_publisher_key = safe_str(selected_row["PublisherKey"])
                st.rerun()
        else:
            st.info("No rows match your search.")

with tab_analytics:
    with st.container(border=True):
        st.markdown("### Progress by priority bucket")

        bucket_summary = (
            queue_merged.groupby("priority_bucket", dropna=False)
            .agg(total=("PublisherKey", "count"), reviewed=("is_reviewed", "sum"))
            .reset_index()
        )
        bucket_summary["remaining"] = bucket_summary["total"] - bucket_summary["reviewed"]
        bucket_summary["completion_rate"] = (bucket_summary["reviewed"] / bucket_summary["total"]).round(4)
        bucket_summary["priority_bucket"] = bucket_summary["priority_bucket"].apply(
            lambda x: get_bucket_meta(safe_str(x))["label"]
        )

        st.dataframe(bucket_summary.sort_values("total", ascending=False), use_container_width=True, hide_index=True)
        st.bar_chart(bucket_summary.set_index("priority_bucket")[["reviewed", "remaining"]], use_container_width=True)

        st.markdown("---")
        st.markdown("### Decision summary")

        if not decisions.empty:
            c1, c2 = st.columns(2)

            with c1:
                label_counts = decisions["reviewed_cluster_label"].value_counts(dropna=False).rename_axis("label").reset_index(name="count")
                label_counts["label"] = label_counts["label"].apply(lambda x: REVIEW_LABELS_DISPLAY.get(safe_str(x), safe_str(x)))
                st.dataframe(label_counts, use_container_width=True, hide_index=True)
                st.bar_chart(label_counts.set_index("label"), use_container_width=True)

            with c2:
                outcome_counts = decisions["review_outcome"].value_counts(dropna=False).rename_axis("outcome").reset_index(name="count")
                outcome_counts["outcome"] = outcome_counts["outcome"].apply(lambda x: OUTCOME_LABELS.get(safe_str(x), safe_str(x)))
                st.dataframe(outcome_counts, use_container_width=True, hide_index=True)
                st.bar_chart(outcome_counts.set_index("outcome"), use_container_width=True)
        else:
            st.info("Analytics will appear once reviewers start saving decisions.")

with tab_help:
    with st.container(border=True):
        st.markdown("### How to review")
        st.markdown(
            """
            For each publisher, decide whether it genuinely belongs in the **Influencer / Content Creator** cluster.

            A publisher **belongs** if its value is mainly driven by:
            - a creator or creator-led identity
            - a creator’s audience or personal brand
            - social/content engagement tied to that person or creator identity

            A publisher **does not belong** if it is mainly:
            - a business or company
            - an agency or network
            - a traditional media/editorial publisher without creator-led identity
            - a utility or non-creator site

            Use **Unsure** only if there is not enough evidence to make a confident decision.
            """
        )

        st.markdown("---")
        st.markdown("### What the priority buckets mean")

        for bucket_key in queue_merged["priority_bucket"].dropna().unique().tolist():
            meta = get_bucket_meta(bucket_key)
            st.markdown(
                f"""
                <div class="legend-card">
                    <div class="legend-title">{meta["label"]}</div>
                    <div class="legend-text">{meta["summary"]}</div>
                    <div class="legend-focus"><strong>What to check:</strong> {meta["reviewer_focus"]}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )