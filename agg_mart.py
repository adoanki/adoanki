from datetime import datetime
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from modules.common_function import get_spark_session
from modules.common_function import read_config


######################################
########Aggreagate Mart###############
######################################
agg_group_by_cols = ["REPORTING_REGULATOR_CODE", "RISK_TAKER_LL_RRU","RISK_ON_LL_RRU","RISK_TAKER_GS_CODE","RISK_ON_GS_CODE",
                    "ILE_COUNTERPARTY_CISCODE", "ILE_RISK_CLASS", "DIVISION", "COREP_PRODUCT_CATEGORY", 
                    "REGULATORY_PRODUCT_TYPE", "COREP_STD_ASSET_CLASS_RK", "B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE",
                    "B2_STD_ASSET_CLASS_BASEL_EXPOSURE_TYPE", "B2_IRB_ASSET_CLASS_REG_EXPOSURE_SUB_TYPE", "B2_IRB_ASSET_CLASS_REG_EXPOSURE_TYPE", 
                    "B2_STD_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE", "UNSETTLED_PERIOD_CODE", "UNSETTLED_FACTOR_CODE", "INCORP_P3_COUNTRY_DESC", "cost_centre", "OP_P3_COUNTRY_DESC", 
                    "ADJUST_FLAG", "BASEL_DATA_FLAG", "RWA_APPROACH_CODE", "ACLM_PRODUCT_TYPE", "RETAIL_OFF_BAL_SHEET_FLAG", "DEFAULT_FUND_CONTRIB_INDICATOR", "FLOW_TYPE", 
                    "SME_FLAG", "INTERNAL_TRANSACTION_FLAG", "IRB_RISK_WEIGHT_RATIO", "STD_RISK_WEIGHT_RATIO", "SLOTTING_RESIDUAL_MATURITY", "LRFE_UFE_FLAG",
                    "RWA_CALC_METHOD", "OBLIGOR_TYPE", "EXTERNAL_COUNTERPARTY_QCCP_FLAG", "DOUBLE_DEFAULT_FLAG", "TRADING_BOOK_FLAG", "REPORTING_TYPE_CODE", 
                    "B3_CVA_CC_ADV_FLAG", "B2_STD_CCF", "B2_STD_CCF_LESS_THAN_YEAR", "B2_STD_CCF_MORE_THAN_YEAR", 
                    "B2_IRB_OBSRVD_NEW_DEFAULT_FLAG", "B2_STD_OBSRVD_NEW_DEFAULT_FLAG", 
                    "CRD4_REP_IRB_FLAG", "CRD4_REP_IRBEQ_FLAG", "CRD4_REP_STD_FLAG", "CRD4_REP_STDFLOW_FLAG", "CRD4_REP_SETTL_FLAG", 
                    "CRD4_REP_CVA_FLAG", "CRD4_SOV_SUBSTITUTION_FLAG", "UNLIKELY_PAY_FLAG", "EXTERNAL_COUNTERPARTY_ID", 
                    "BRANCH_CISCODE", "STD_EXPOSURE_DEFAULT_FLAG", "CRD4_STD_RP_SME_FLAG", "CRD4_IRB_RP_SME_FLAG", 
                    "STD_SME_DISCOUNT_APP_FLAG", "IRB_SME_DISCOUNT_APP_FLAG", "TRANSITIONAL_PORTFOLIO_FLAG", 
                    "MATURITY_BAND_DESC", "COREP_IRB_ASSET_CLASS_RK", "COREP_CRM_REPORTING_FLAG", 
                    "B2_IRB_INTEREST_AT_DEFAULT_GBP", "PERS_ACC_FLAG", "B3_SME_RP_FLAG", "B3_SME_DISCOUNT_FLAG", 
                    "CRD4_MEMO_ITEMS_EXPOSURE_TYPE", "CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_TYPE", 
                    "IG_EXCLUSION_FLAG", "NEW_IN_DEFAULT_FLAG", "RETAIL_POOL_SME_FLAG", "SYS_CODE", "CS_PROXY_USED_FLAG", 
                    "MARKET_RISK_SUB_TYPE", "DEAL_TYPE", "STS_SECURITISATION", "STS_SEC_QUAL_CAP_TRTMNT", 
                    "STS_SEC_APPROACH_CODE", "IFRS9_FINAL_STAGE", "IFRS9_STAGE_3_TYPE", "IFRS9_ECL_CURRENCY_CODE", 
                    "RISK_TYPE", "REP_PROVISION_TYPE", "C33_EXPOSURE_SECTOR", "FINREP_COUNTERPARTY_SECTOR", "IAS39_CLASS", 
                    "CRR2_501A_DISCOUNT_FLAG", "MORTGAGES_FLAG", "BASEL_SECURITY_SUB_TYPE", "SYS_PROTECTION_TYPE","MGS",
                    "PD_BAND_CODE","YEAR_MONTH", "CRD4_MEMO_ITEMS_ASSET_CLASS_REG_EXPOSURE_TYPE", "CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE", "MEMO_B2_STD_TYPE",
                    "B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE", "B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE", "B2_STD_PRE_DFLT_ASSET_CLASS_TYPE", "COST_CENTRE_TYPE",
                    "INCORP_COUNTRY_DESC", "OP_COUNTRY_DESC", "corep_incorp_p3_country_desc", "corep_incorp_country_desc", "RISK_TAKER_LL_RRU_DESC", "RISK_ON_LL_RRU_DESC", "B2_STD_EXPOSURE_VALUE",
                    "B2_STD_FULLY_ADJ_EXP_CCF_50", "B2_STD_FULLY_ADJ_EXP_CCF_20", "B2_STD_FULLY_ADJ_EXP_CCF_0", "B2_STD_EFF_FIN_COLL_AMT", "B2_STD_NETTED_COLL_GBP", "B2_STD_CRM_TOT_OUTFLOW", "B2_STD_CRM_TOT_INFLOW",
                    "B2_STD_PROVISIONS_AMT", "IMP_WRITE_OFF_AMT"
                    ]

agg_input_cols = [ "EAD_LGD_NUMERATOR", "EAD_PD_NUMERATOR", "B2_IRB_ORIG_EXP_PRE_CON_FACTOR",
                    "B2_IRB_PROVISIONS_AMT", "EAD_POST_CRM_AMT", "EAD_LGD_NUMERATOR","RWA_POST_CRM_AMOUNT",
                    "B2_APP_EXPECTED_LOSS_AMT","ADJUST_FLAG","UNSETTLED_AMOUNT","UNSETTLED_PRICE_DIFFERENCE_AMT"] + agg_group_by_cols;


aggl1_group_by_cols = ["YEAR_MONTH", "RISK_TAKER_LL_RRU","RISK_ON_LL_RRU","RISK_TAKER_GS_CODE","RISK_ON_GS_CODE", 
                    "DIVISION", "COREP_IRB_ASSET_CLASS_RK", "COREP_STD_ASSET_CLASS_RK", 
                    "B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE", "B2_STD_ASSET_CLASS_BASEL_EXPOSURE_TYPE", 
                    "B2_IRB_ASSET_CLASS_REG_EXPOSURE_SUB_TYPE", "B2_IRB_ASSET_CLASS_REG_EXPOSURE_TYPE", 
                    "B2_STD_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE", "UNSETTLED_PERIOD_CODE", "INCORP_P3_COUNTRY_DESC", 
                    "OP_P3_COUNTRY_DESC",  "ADJUST_FLAG", "REPORTING_REGULATOR_CODE", "BASEL_DATA_FLAG", 
                    "RWA_APPROACH_CODE", "ACLM_PRODUCT_TYPE", "DEFAULT_FUND_CONTRIB_INDICATOR", "SME_FLAG",
                        "INTERNAL_TRANSACTION_FLAG", "RWA_CALC_METHOD", "TRADING_BOOK_FLAG", 
                    "REPORTING_TYPE_CODE", "CRD4_REP_IRB_FLAG", "CRD4_REP_IRBEQ_FLAG", 
                    "CRD4_REP_STD_FLAG", "CRD4_REP_STDFLOW_FLAG", "CRD4_REP_SETTL_FLAG", 
                    "STD_SME_DISCOUNT_APP_FLAG", "IRB_SME_DISCOUNT_APP_FLAG", "PERS_ACC_FLAG", 
                    "B3_SME_RP_FLAG", "B3_SME_DISCOUNT_FLAG", "IG_EXCLUSION_FLAG","CRD4_MEMO_ITEMS_EXPOSURE_TYPE", 
                    "RETAIL_POOL_SME_FLAG", "SYS_CODE", "DEAL_TYPE", "STS_SECURITISATION", 
                    "STS_SEC_QUAL_CAP_TRTMNT",   "STS_SEC_APPROACH_CODE", "IFRS9_FINAL_STAGE", 
                    "IFRS9_STAGE_3_TYPE", "IFRS9_ECL_CURRENCY_CODE", "REP_PROVISION_TYPE", 
                    "C33_EXPOSURE_SECTOR", "FINREP_COUNTERPARTY_SECTOR", "IAS39_CLASS", 
                    "CRR2_501A_DISCOUNT_FLAG", "MORTGAGES_FLAG", "SYS_PROTECTION_TYPE","CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_TYPE", 
                    "MGS", "PD_BAND_CODE", "CRD4_MEMO_ITEMS_ASSET_CLASS_REG_EXPOSURE_TYPE", "CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE", "MEMO_B2_STD_TYPE",
                    "B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE", "B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE", "B2_STD_PRE_DFLT_ASSET_CLASS_TYPE", "COST_CENTRE_TYPE"
                    ,"INCORP_COUNTRY_DESC", "OP_COUNTRY_DESC", "corep_incorp_p3_country_desc", "corep_incorp_country_desc", "RISK_TAKER_LL_RRU_DESC", "RISK_ON_LL_RRU_DESC"
                    ]

if __name__ == "__main__":
    
    

    #initializing spark session

    spark = get_spark_session()
    config = read_config(spark)

    # Parameters from config file
    reporting_date = config["Input_parameters"]["reporting_date"]
    p_reporting_date = datetime.strptime(reporting_date, '%Y-%m-%dT%H::%M::%S.%f').strftime('%Y-%m-%d')
    v_year_month_rk = datetime.strptime(reporting_date,'%Y-%m-%dT%H::%M::%S.%f').strftime('%Y%m')
    p_data_set = 'A'
    srtd_mart_db = config["Database_details"]["srtd_mart_db"]


    print("\n\n***** PASSED PARAMETERS******\n\n")

    print(f"\n\n Reporting_date : {reporting_date}")
    print(f"\n\n p_reporting_date : {p_reporting_date}")
    print(f"\n\n v_year_month_rk : {v_year_month_rk}")
    print(f"\n\n srtd_mart_db : {srtd_mart_db}")
    

    ######################################
    ########Aggreagate Mart###############
    ######################################

    crd4_mart_df = spark.sql(f"select * from {srtd_mart_db}.rwa_reporting_mart where year_month = {v_year_month_rk} and ADJUST_FLAG = '{p_data_set}'");
    agg_input_df = crd4_mart_df.select(agg_input_cols).distinct();
    agg_input_df = agg_input_df.persist(StorageLevel.MEMORY_AND_DISK_SER);
    
    
    #Aggregation on required dataset either ajusted or unadjusted
    grouped_agg_df = agg_input_df.groupBy(agg_group_by_cols).agg(sum("B2_IRB_ORIG_EXP_PRE_CON_FACTOR").alias("B2_IRB_ORIG_EXP_PRE_CON_FACTOR_AGG"),
                                                    sum("B2_IRB_PROVISIONS_AMT").alias("B2_IRB_PROVISIONS_AMT_AGG"),
                                                    sum("EAD_POST_CRM_AMT").alias("EAD_POST_CRM_AMT_AGG"),
                                                    sum("EAD_LGD_NUMERATOR").alias("EAD_LGD_NUMERATOR_AGG"),
                                                    sum("EAD_PD_NUMERATOR").alias("EAD_PD_NUMERATOR_AGG"),
                                                    sum("RWA_POST_CRM_AMOUNT").alias("RWA_POST_CRM_AMOUNT_AGG"),            
                                                    sum("B2_APP_EXPECTED_LOSS_AMT").alias("B2_APP_EXPECTED_LOSS_AMT_AGG"),
                                                    sum("UNSETTLED_AMOUNT").alias("UNSETTLED_AMOUNT_AGG"),
                                                    sum("UNSETTLED_PRICE_DIFFERENCE_AMT").alias("UNSETTLED_PRICE_DIFFERENCE_AMT_AGG"));

    #Aggregation on total year month without considering dataset type
    further_agg_df = grouped_agg_df.groupBy(agg_group_by_cols).agg(sum("B2_IRB_ORIG_EXP_PRE_CON_FACTOR_AGG").alias("B2_IRB_ORIG_EXP_PRE_CON_FACTOR"),
                                                    sum("B2_IRB_PROVISIONS_AMT_AGG").alias("B2_IRB_PROVISIONS_AMT"),
                                                    sum("EAD_POST_CRM_AMT_AGG").alias("EAD_POST_CRM_AMT"),
                                                    sum("EAD_LGD_NUMERATOR_AGG").alias("EAD_LGD_NUMERATOR"),
                                                    sum("EAD_PD_NUMERATOR_AGG").alias("EAD_PD_NUMERATOR"),
                                                    sum("RWA_POST_CRM_AMOUNT_AGG").alias("RWA_POST_CRM_AMOUNT"),               
                                                    sum("B2_APP_EXPECTED_LOSS_AMT_AGG").alias("B2_APP_EXPECTED_LOSS_AMT"),
                                                    sum("UNSETTLED_AMOUNT_AGG").alias("UNSETTLED_AMOUNT"),
                                                    sum("UNSETTLED_PRICE_DIFFERENCE_AMT_AGG").alias("UNSETTLED_PRICE_DIFFERENCE_AMT")
                                                                ).persist(StorageLevel.MEMORY_AND_DISK_SER);

    #Creation of AGG_COREP_CRD4_MART Table 

    print("\n\nWRITING AGGREGATE_MART DATA HERE : srtd_mart.agg_corep_crd4_mart\n\n")

    further_agg_df = further_agg_df.select("risk_taker_ll_rru", "risk_on_ll_rru", "risk_taker_gs_code", "risk_on_gs_code", "ile_counterparty_ciscode", "ile_risk_class", "division", "corep_product_category", "regulatory_product_type", "corep_std_asset_class_rk", "b2_std_pre_dflt_asset_class_basel_exposure_type", "b2_std_asset_class_basel_exposure_type", "b2_irb_asset_class_reg_exposure_sub_type", "b2_irb_asset_class_reg_exposure_type", "b2_std_asset_class_basel_exposure_sub_type", "unsettled_period_code", "unsettled_factor_code", "incorp_p3_country_desc", "cost_centre", "op_p3_country_desc", "adjust_flag", "basel_data_flag", "rwa_approach_code", "aclm_product_type", "retail_off_bal_sheet_flag", "default_fund_contrib_indicator", "flow_type", "sme_flag", "internal_transaction_flag", "irb_risk_weight_ratio", "std_risk_weight_ratio", "slotting_residual_maturity", "lrfe_ufe_flag", "rwa_calc_method", "obligor_type", "external_counterparty_qccp_flag", "double_default_flag", "trading_book_flag", "reporting_type_code", "b3_cva_cc_adv_flag", "b2_std_ccf", "b2_std_ccf_less_than_year", "b2_std_ccf_more_than_year", "b2_irb_obsrvd_new_default_flag", "b2_std_obsrvd_new_default_flag", "crd4_rep_irb_flag", "crd4_rep_irbeq_flag", "crd4_rep_std_flag", "crd4_rep_stdflow_flag", "crd4_rep_settl_flag", "crd4_rep_cva_flag", "crd4_sov_substitution_flag", "unlikely_pay_flag", "external_counterparty_id", "branch_ciscode", "std_exposure_default_flag", "crd4_std_rp_sme_flag", "crd4_irb_rp_sme_flag", "std_sme_discount_app_flag", "irb_sme_discount_app_flag", "transitional_portfolio_flag", "maturity_band_desc", "corep_irb_asset_class_rk", "corep_crm_reporting_flag", "b2_irb_interest_at_default_gbp", "pers_acc_flag", "b3_sme_rp_flag", "b3_sme_discount_flag", "crd4_memo_items_exposure_type", "crd4_memo_items_asset_class_basel_exposure_type", "ig_exclusion_flag", "new_in_default_flag", "retail_pool_sme_flag", "sys_code", "cs_proxy_used_flag", "market_risk_sub_type", "deal_type", "sts_securitisation", "sts_sec_qual_cap_trtmnt", "sts_sec_approach_code", "ifrs9_final_stage", "ifrs9_stage_3_type", "ifrs9_ecl_currency_code", "risk_type", "rep_provision_type", "c33_exposure_sector", "finrep_counterparty_sector", "ias39_class", "crr2_501a_discount_flag", "mortgages_flag", "basel_security_sub_type", "sys_protection_type", "mgs", "pd_band_code", "crd4_memo_items_asset_class_reg_exposure_type", "crd4_memo_items_asset_class_basel_exposure_sub_type", "memo_b2_std_type", "b2_std_pre_dflt_asset_class_basel_exposure_sub_type", "b2_std_pre_dflt_asset_class_reg_exposure_type", "b2_std_pre_dflt_asset_class_type", "cost_centre_type", "incorp_country_desc", "op_country_desc", "corep_incorp_p3_country_desc", "corep_incorp_country_desc", "risk_taker_ll_rru_desc", "risk_on_ll_rru_desc", "b2_std_exposure_value", "b2_std_fully_adj_exp_ccf_50", "b2_std_fully_adj_exp_ccf_20", "b2_std_fully_adj_exp_ccf_0", "b2_std_eff_fin_coll_amt", "b2_std_netted_coll_gbp", "b2_std_crm_tot_outflow", "b2_std_crm_tot_inflow", "b2_std_provisions_amt", "imp_write_off_amt", "b2_irb_orig_exp_pre_con_factor", "b2_irb_provisions_amt", "ead_post_crm_amt", "ead_lgd_numerator", "ead_pd_numerator", "rwa_post_crm_amount", "b2_app_expected_loss_amt", "unsettled_amount", "unsettled_price_difference_amt", "year_month", "reporting_regulator_code")
    
    further_agg_df.write.format("parquet").mode("overwrite").insertInto(srtd_mart_db + '.' + "agg_corep_crd4_mart",overwrite=True)
    #Null Handling of all the being aggregated columns  before L1_Aggregation
    agg_corep_crd4 = spark.sql(f"select * from {srtd_mart_db}.agg_corep_crd4_mart where year_month = {v_year_month_rk} and ADJUST_FLAG = '{p_data_set}'").fillna(value = 0,subset = ["EAD_LGD_NUMERATOR", "EAD_PD_NUMERATOR", "B2_IRB_ORIG_EXP_PRE_CON_FACTOR",
                    "B2_IRB_PROVISIONS_AMT", "EAD_POST_CRM_AMT", "EAD_LGD_NUMERATOR","RWA_POST_CRM_AMOUNT",
                    "B2_APP_EXPECTED_LOSS_AMT","UNSETTLED_AMOUNT","UNSETTLED_PRICE_DIFFERENCE_AMT"]);

    ######################################
    ########Aggreagate Mart L1############
    ######################################
    agg_corep_crd4_l1 = agg_corep_crd4.groupBy(aggl1_group_by_cols).agg(sum("B2_IRB_ORIG_EXP_PRE_CON_FACTOR").alias("B2_IRB_ORIG_EXP_PRE_CON_FACTOR"),
                                                sum("B2_IRB_PROVISIONS_AMT").alias("B2_IRB_PROVISIONS_AMT"),
                                                sum("EAD_POST_CRM_AMT").alias("EAD_POST_CRM_AMT"),
                                                sum("EAD_LGD_NUMERATOR").alias("EAD_LGD_NUMERATOR"),
                                                sum("EAD_PD_NUMERATOR").alias("EAD_PD_NUMERATOR"),
                                                sum("RWA_POST_CRM_AMOUNT").alias("RWA_POST_CRM_AMOUNT"),                    
                                                sum("B2_APP_EXPECTED_LOSS_AMT").alias("B2_APP_EXPECTED_LOSS_AMT"),
                                                sum("UNSETTLED_AMOUNT").alias("UNSETTLED_AMOUNT"),
                                                sum("UNSETTLED_PRICE_DIFFERENCE_AMT").alias("UNSETTLED_PRICE_DIFFERENCE_AMT")).persist(StorageLevel.MEMORY_AND_DISK_SER);

    #Creation of AGG_COREP_CRD4_MART_L1 Table

    print("\n\nWRITING AGGREGATE_MART_L1 DATA HERE : srtd_mart.agg_corep_crd4_mart_l1\n\n")

    agg_corep_crd4_l1 = agg_corep_crd4_l1.select("risk_taker_ll_rru", "risk_on_ll_rru", "risk_taker_gs_code", "risk_on_gs_code", "division", "corep_irb_asset_class_rk", "corep_std_asset_class_rk", "b2_std_pre_dflt_asset_class_basel_exposure_type", "b2_std_asset_class_basel_exposure_type", "b2_irb_asset_class_reg_exposure_sub_type", "b2_irb_asset_class_reg_exposure_type", "b2_std_asset_class_basel_exposure_sub_type", "unsettled_period_code", "incorp_p3_country_desc", "op_p3_country_desc", "adjust_flag", "basel_data_flag", "rwa_approach_code", "aclm_product_type", "default_fund_contrib_indicator", "sme_flag", "internal_transaction_flag", "rwa_calc_method", "trading_book_flag", "reporting_type_code", "crd4_rep_irb_flag", "crd4_rep_irbeq_flag", "crd4_rep_std_flag", "crd4_rep_stdflow_flag", "crd4_rep_settl_flag", "std_sme_discount_app_flag", "irb_sme_discount_app_flag", "pers_acc_flag", "b3_sme_rp_flag", "b3_sme_discount_flag", "ig_exclusion_flag", "crd4_memo_items_exposure_type", "retail_pool_sme_flag", "sys_code", "deal_type", "sts_securitisation", "sts_sec_qual_cap_trtmnt", "sts_sec_approach_code", "ifrs9_final_stage", "ifrs9_stage_3_type", "ifrs9_ecl_currency_code", "rep_provision_type", "c33_exposure_sector", "finrep_counterparty_sector", "ias39_class", "crr2_501a_discount_flag", "mortgages_flag", "sys_protection_type", "crd4_memo_items_asset_class_basel_exposure_type", "mgs", "pd_band_code", "crd4_memo_items_asset_class_reg_exposure_type", "crd4_memo_items_asset_class_basel_exposure_sub_type", "memo_b2_std_type", "b2_std_pre_dflt_asset_class_basel_exposure_sub_type", "b2_std_pre_dflt_asset_class_reg_exposure_type", "b2_std_pre_dflt_asset_class_type", "cost_centre_type", "incorp_country_desc", "op_country_desc", "corep_incorp_p3_country_desc", "corep_incorp_country_desc", "risk_taker_ll_rru_desc", "risk_on_ll_rru_desc", "b2_irb_orig_exp_pre_con_factor", "b2_irb_provisions_amt", "ead_post_crm_amt", "ead_lgd_numerator", "ead_pd_numerator", "rwa_post_crm_amount", "b2_app_expected_loss_amt", "unsettled_amount", "unsettled_price_difference_amt", "year_month", "reporting_regulator_code")

    agg_corep_crd4_l1.write.format("parquet").mode("overwrite").insertInto(srtd_mart_db + '.' + "agg_corep_crd4_mart_l1",overwrite=True)