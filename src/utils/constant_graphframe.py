# constant_graphframe.py
# ---------------------------------------------------------------------
# Centralized constants/config for GraphFrame feature generation.
# Merge of the three screenshots you shared (ordered by line numbers).
# ---------------------------------------------------------------------

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------------------
# Profiles / environment
# ---------------------------------------------------------------------
active = "dev"

# ---------------------------------------------------------------------
# Datasets & time windows
# training dataset: 2 yrs entities -> requires 3 yrs of trxn
# prod dataset:     ~1 mn entities -> requires 13 months of trxn
# ---------------------------------------------------------------------

# filters / staging envs
stg_ext = "allxx"                      # TODO: confirm exact value from your repo
final_tbl_suffix = "_4FCB_v2_tmp"      # e.g., "…_4FCB_v2_tmp"
final_tbl = f"{stg_ext}.{final_tbl_suffix}"

# Vintages (as per “OLD FUNCTION” logic in screenshots)
vintage_start_dt = "2022-06-01"  # TODO: confirm — visible in screenshot header
vintage_end_dt = (
    datetime.strptime(vintage_start_dt, "%Y-%m-%d") + relativedelta(months=31)
).strftime("%Y-%m-%d")

# Extended lookback horizon derived for training/prod mixes
start_dt = (
    datetime.strptime(vintage_start_dt, "%Y-%m-%d") - relativedelta(months=12)
).strftime("%Y-%m-%d")
end_dt = vintage_end_dt

# Example “run date” (as seen in comments)
run_date = "2024-09-30"  # TODO: set to your job’s run date if needed

# Convenience cut points derived from run_date if you use them elsewhere
# vintage_dt = (datetime.strptime(run_date, "%Y-%m-%d").replace(day=1) - relativedelta(days=1)).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------
# Storage roots / paths (as commented in screenshot)
# ---------------------------------------------------------------------
qtx_br_root = (
    "/Volumes/111507_iridiscovery_ctg_prod_exp/default/batchresolverpi3/run4_20250605/3YearTrxn/"
    # example comment: "2022-06-30 to 2025-05-31"
)

# ---------------------------------------------------------------------
# Schemas / databases
# ---------------------------------------------------------------------
src_schema = "111507_ctg_prod_cc_rtn_trusted_view"
old_des_schema = "111507_iridiscovery_ctg_prod_exp.ri_ee_risk_entity"  # legacy
des_schema = "111507_datascisar_ctg_prod_exp.ri_ee_risk_model_dev"     # dev
prd_schema = "111507_datascisar_ctg_prod_exp.0235249"                  # example id  (TODO confirm)

# ---------------------------------------------------------------------
# Sources (views / tables)
# ---------------------------------------------------------------------
wire_tbl = "rpt_wire_v"
acct_tbl_exact_v = "acct_v"  # TODO confirm exact name in your environment

# ---------------------------------------------------------------------
# Staging tables from QTX (as in screenshot comments)
# ---------------------------------------------------------------------
entity_raw_driver = "ee_entityid_driver"          # flattened BR output — all entity Data from QTX
entity_rec_driver = "ee_entityid_driver_rec"      # example: record-level driver (if used)
resolved_entities = "resolved_entities"
final_tbl_base = "ee_stg_final_driver"

# FCB entities / drivers (as seen around lines ~50+ in screenshot)
fcb_entities_tbl = "fcb_entities"                 # flattened FCB entities (FCTM-QTX)
final_tmp_tbl = "mtx_trxn_final_driver_tmp"       # staging driver (temp) with all columns
final_tbl_name = "stg04_entity_final_driver"      # final staging driver table

# ---------------------------------------------------------------------
# Additional resources (CSV/lookup paths from repo tree comments)
# ---------------------------------------------------------------------
naics_src = "../../resources/NAICS_to_SIC.csv"           # example path
naics_risk_score = "../../resources/naics_risk_score.csv"
consolidated_search_terms = "../../resources/consolidated_search_terms.csv"

# ---------------------------------------------------------------------
# Feature tables (downstream outputs)
# ---------------------------------------------------------------------
wirecheck_pair = "ee_feat_worldcheck_pair_trxn_features"
qtx_flags = "ee_feat_qtx_flags_features"
all_feats = "ee_all_feature"  # combined

# ---------------------------------------------------------------------
# Domain filters & lookups
# ---------------------------------------------------------------------
exclude_sic = ["7389"]   # TODO: add more if you use them

fcb_eccl_lst = ("0000008250","0000015400","0186927029","0186675075","0001181130")  # sample from screenshot
XA_check_cols = ("orig_remitter_id_type", "benef_type", "tp_benef_type", "tp_origin_type")

occtTypeMap = {
    "origin": {
        "TP_ORIGIN_TYPE": ("TP_ORIGIN_ACCT_ID","TP_ORIGIN_NAME","TP_ORIGIN_COUNTRY"),
        "ORIG_REMITTER_ID_TYPE": ("ORIG_REMITTER_ACCT_ID","ORIG_REMITTER_NAME","ORIG_REMITTER_COUNTRY"),
    },
    "benef": {
        "BENEF_TYPE": ("BENEF_ACCT_ID","BENEF_NAME","BENEF_COUNTRY"),
        "TP_BENEF_TYPE": ("TP_BENEF_ACCT_ID","TP_BENEF_NAME","TP_BENEF_COUNTRY"),
    },
}

pivot_lookup_cols = [
    "EXT_ACCOUNT_ID","EXT_ACCT_TAG","FCB_ACCOUNT_ID","POSTING_DATE","TRXN_REF_ID",
    "CR_DR","VINTAGE_DT","POSTING_MONTH","INSTN_TYPE","INSTN_ID","INSTN_ACCT_ID",
]

# ---------------------------------------------------------------------
# Older country lists (kept for parity; from screenshots lines ~90–110)
# ---------------------------------------------------------------------
old_vhrg_cntry_lst = [
    # e.g., ["AF","AO","AZ","BD","BY","BI","CM","CF","TD","IQ","KM", ...]
    # TODO: fill from your original file if still referenced
]
old_hrg_cntry_lst = [
    # e.g., ["AE","AI","AG","AR","AM","BS","BH", ...]
    # TODO: fill if needed
]

# Risky account prefixes (from screenshot line ~110)
risky_prefixes = ["NRA", "OSA", "FIN"]  # TODO: verify

# ---------------------------------------------------------------------
# Sanction-evasion country list (screenshot lines ~112–118)
# ISO3 mapping for convenience
# ---------------------------------------------------------------------
sanction_evasion_country_lst = {
    "Armenia": "ARM", "Azerbaijan": "AZE", "Belarus": "BLR", "China": "CHN",
    "Hong Kong": "HKG", "Kazakhstan": "KAZ", "Kyrgyzstan": "KGZ", "Malaysia": "MYS",
    "Moldova": "MDA", "Mongolia": "MNG", "Serbia": "SRB", "Thailand": "THA",
    "Turkey": "TUR", "United Arab Emirates": "ARE", "Uzbekistan": "UZB",
}

# ---------------------------------------------------------------------
# 2024 Sep updated HRG/VHRG lists (screenshots ~118–123, 120–122)
# Two-level risk sets used by neighbor & centrality features.
# ---------------------------------------------------------------------
HIGH_RISK_COUNTRIES = [
    # ISO3 codes — from screenshot. Keep exactly as maintained in your policy set.
    # NOTE: I include a representative subset below. Please replace with the full
    # list from your image or your source-of-truth file to avoid drift.
    "ALB","DZA","ATA","ATG","ARG","ARM","BHS","BHR","BRB","BLZ","BEN","BOL","BES","BFA","CPV",
    "CHN","HKG","CIV","CMR","COL","CRI","CUB","DJI","DMA","DOM","ECU","EGY","SLV","SWZ","ETH",
    "GAB","GMB","GEO","GHA","GIB","GTM","GUY","HND","IND","IDN","IRN","IRQ","JAM","JOR","KHM",
    "LAO","LBN","LBR","LBY","LTU","MAC","MKD","MDG","MLT","MDV","MLI","MRT","MUS","MEX","MDA",
    "MNG","MAR","MOZ","MMR","NAM","NPL","NIC","NER","NGA","OMN","PAK","PAN","PER","PHL","QAT",
    "SAU","SEN","SRB","SYC","SLE","SOM","SSD","LKA","SDN","SUR","SYR","TZA","THA","TGO","TTO",
    "TUN","TUR","UGA","UKR","URY","UZB","VCT","VEN","VNM","YEM","ZAF","ZMB",
    # TODO: replace with the exact full list from your image/repo
]

VERY_HIGH_RISK_COUNTRIES = [
    # ISO3 codes — from screenshot lines 120–122 (subset shown; replace with full list)
    "AFG","AGO","AZE","BGD","CAF","TCD","COM","COD","COG","CUB","DPRK","ERI","GIN","GNB",
    "HTI","IRN","IRQ","KEN","KGZ","LBN","LBR","LBY","MDG","MLI","MOZ","MMR","NIC","NGA","PAK",
    "PSE","RUS","SOM","SSD","SDN","SYR","TJK","TLS","TUR","UGA","UKR","UZB","VEN","YEM","ZWE",
    # TODO: replace with exact full list from your policy set
]

# ---------------------------------------------------------------------
# Wire (payment) source columns (screenshot 123–171)
# Used to select & normalize wire rows before graph construction.
# ---------------------------------------------------------------------
wire_src_cols = [
    "TRXN_REF_ID",
    "BENEF_ACCT_ID","BENEF_CC_CD","BENEF_COUNTRY","BENEF_NAME","BENEF_TYPE",
    "ORIG_REMITTER_ACCT_ID","ORIG_REMITTER_AMT_ACTIVITY","ORIG_REMITTER_CCY_CD",
    "ORIG_REMITTER_COUNTRY","ORIG_REMITTER_NAME","ORIG_REMITTER_ID_TYPE",
    "ORIG_REMITTER_TOD_TYPE","POSTING_DATE",
    "RCV_INSTN_ACCT_ID","RCV_INSTN_AMT_ACTIVITY","RCV_INSTN_CCY_CD","RCV_INSTN_COUNTRY",
    "RCV_INSTN_ID","RCV_INSTN_NAME","RCV_INSTN_TYPE",
    "SENDING_INSTN_ACCT_ID","SENDING_INSTN_AMT_ACTIVITY","SENDING_INSTN_CCY_CD",
    "SENDING_INSTN_COUNTRY","SENDING_INSTN_ID","SENDING_INSTN_NAME","SENDING_INSTN_TYPE",
    "TP_BENEF_ACCT_ID","TP_BENEF_CC_CD","TP_BENEF_COUNTRY","TP_BENEF_NAME","TP_BENEF_TYPE",
    "TP_ORIGIN_ACCT_ID","TP_ORIGIN_AMT_ACTIVITY","TP_ORIGIN_CCY_CD","TP_ORIGIN_COUNTRY",
    "TP_ORIGIN_NAME","TP_ORIGIN_TYPE",
    "TRXN_AMOUNT","TRXN_CURRENCY_CD","TRXN_GROUP","TRANSACTION_TYPE",
    # If your model uses more, add here per screenshot tail
]

# ---------------------------------------------------------------------
# Expose a small helper bundle for imports like:
#   from src.utils.util_graphframe import constant_graphframe
# ---------------------------------------------------------------------
class constant_graphframe:
    HIGH_RISK_COUNTRIES = HIGH_RISK_COUNTRIES
    VERY_HIGH_RISK_COUNTRIES = VERY_HIGH_RISK_COUNTRIES
    RISKY_PREFIXES = risky_prefixes
    WIRE_SRC_COLS = wire_src_cols
    SANCTION_EVASION_COUNTRIES = sanction_evasion_country_lst

    # Useful date range controls
    start_date = start_dt
    end_date = end_dt
    run_date = run_date

    # Tables/paths (commonly referenced by other modules)
    final_tbl = final_tbl
    final_tbl_name = final_tbl_name
    final_tmp_tbl = final_tmp_tbl
    wire_tbl = wire_tbl
    qtx_br_root = qtx_br_root