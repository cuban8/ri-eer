# src/config_graphframe.py

import sys
sys.path.insert(0, "../src")

from src.utils import constant_graphframe as constant


class Config:
    def __init__(self):
        print("Initiating Parser Arguments...")

        # -----------------------------------------------------------------
        # Date variables
        # -----------------------------------------------------------------
        # Uncomment if you want to drive start/end dates from constants
        # self.start_date = constant.start_dt
        # self.end_date = constant.end_dt
        # self.vintage_start_dt = constant.vintage_start_dt
        # self.vintage_end_dt = constant.vintage_end_dt

        # -----------------------------------------------------------------
        # HRG/VHRG country lists
        # -----------------------------------------------------------------
        # self.hrg_cntry = constant.hrg_cntry_lst
        # self.vhrg_cntry = constant.vhrg_cntry_lst

        # -----------------------------------------------------------------
        # Source tables
        # -----------------------------------------------------------------
        self.wire_tbl = f"{constant.src_schema}.{constant.wire_tbl}"
        self.acct_tbl = f"{constant.src_schema}.{constant.acct_tbl_exact_v}"

        # -----------------------------------------------------------------
        # Staging tables
        # -----------------------------------------------------------------
        # Original examples from screenshot (kept for reference)
        # self.ntity_driver = "111507_datascisar_ctg_prod_exp.ri_ee_risk_model_prod.ee_entityid_driver"
        # self.ntity_rec_driver = "111507_datascisar_ctg_prod_exp.ri_ee_risk_model_prod.ee_entityid_driver_rec"
        # self.fcb_entities = "111507_datascisar_ctg_prod_exp.ri_ee_risk_model_dev.allfcb_sample_entities"
        # self.final_tmp_tbl = f"{constant.des_schema}.{constant.final_tmp_tbl}{constant.stg_ext}"
        # self.final_tbl = f"{constant.des_schema}.{constant.final_tbl}{constant.stg_ext}"
        # self.search_term_tbl = "111507_datascisar_ctg_prod_exp.ri_ee_risk_model_prod.sar_intel_consolidated_search_terms"

        # Updated staging references (screenshot lines 38–40)
        self.ntity_driver = f"{constant.des_schema}.{constant.stg_ext}{constant.ntity_driver}"
        self.resolved_entities = f"{constant.des_schema}.{constant.stg_ext}{constant.resolved_entities}"
        self.final_tbl = f"{constant.des_schema}.{constant.stg_ext}{constant.final_tbl}"

        # -----------------------------------------------------------------
        # Feature tables
        # -----------------------------------------------------------------
        self.wire_multi = f"{constant.des_schema}.{constant.feat_ext}{constant.wire_multiday}"
        self.crypto_feat = f"{constant.des_schema}.{constant.feat_ext}{constant.crypto_feat}"
        self.pair_feat = f"{constant.des_schema}.{constant.feat_ext}{constant.pair_feat}"
        self.add_pair_feat = f"{constant.des_schema}.{constant.feat_ext}{constant.add_pair_feat}"
        self.network_feat = f"{constant.des_schema}.{constant.feat_ext}{constant.network_feat}"
        self.naics_risk_feat = f"{constant.des_schema}.{constant.feat_ext}{constant.naics_risk_feat}"
        self.worldcheck_pair = f"{constant.des_schema}.{constant.feat_ext}{constant.worldcheck_pair}"
        self.qtx_flags = f"{constant.des_schema}.{constant.feat_ext}{constant.qtx_flags}"
        self.all_feat = f"{constant.des_schema}.{constant.feat_ext}{constant.all_feat}"