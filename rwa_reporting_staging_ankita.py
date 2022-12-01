from pyspark import StorageLevel
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import Row, DoubleType, StringType
from pyspark.sql.functions import col,lit,sum,broadcast, udf,expr
from pyspark.sql.types import *
from modules.common_function import get_spark_session
from modules.common_function import read_config
from os import path, environ, popen


# def get_spark_session():
#     print("\n\n\n\nCreating Spark Session Object..\n")
#     spark = SparkSession.builder.appName("CREATE_INTERFACE").enableHiveSupport().getOrCreate()
#     spark.sparkContext.setLogLevel("ERROR");
#     spark.conf.set("spark.sql.crossJoin.enabled", True);
#     spark.conf.set("spark.sql.parquet.enableVectorizedReader", False);
#     spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict");

#     return spark
# #   end get_spark_session()


#def initialize_db(db_name, spark):
#    value = "use " + db_name
#    spark.sql(value)
#   end of initialize_db()


def populate_exp_staging(spark, reporting_date, scdl_db, srtd_db, wsls_db, rtd_ref_db, srtd_mart_db, stg_exp_tab_name):

    var_query = f"""SELECT date_format('{reporting_date}', 'yyyyMM') rYear_Month_RK ,
                        date_format('{reporting_date}', 'yyyyMMdd') rDay_RK ,
                        CAST ((SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name = 'PRA_DIVERGENCE_EFF_DATE') AS INTEGER)
                        rpradiv_eff_from_yrmnth_rk,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name = 'CRR2_DUAL_REP_EFF_DATE') AS INTEGER)
                        AS rdualrep_eff_from_yrmnth_rk,
                        date ('{reporting_date}') AS v_reporting_date,
                        CAST (
                        (SELECT ASSET_CLASS_RK
                            FROM {srtd_db}.DIM_ASSET_CLASS
                            WHERE     TYPE = 'B2_STD'
                                AND VALID_FROM_DATE <=
                                        date ('{reporting_date}')
                                AND VALID_TO_DATE >
                                        date ('{reporting_date}')
                                AND BASEL_EXPOSURE_TYPE = 'Other items') AS INTEGER)
                        AS v_oth_itm_asset_class_rk"""
    var = spark.sql(var_query);
    var.createOrReplaceTempView("var");
    
    print('''\n\n\n********* var df loaded *****''')

    dim_cntrpty_query = f"""select * from {wsls_db}.dimcntrpty, var where snap_dt in (select max(snap_dt) from {wsls_db}.dimcntrpty) and dimcntrpty.valid_from_date <= var.v_reporting_date and dimcntrpty.valid_to_date > var.v_reporting_date""";
    dim_cntrpty_df = spark.sql(dim_cntrpty_query);
    print("\n\nTotal counterparty records fetched : ", dim_cntrpty_df.count());
    print("\n\n");
    dim_cntrpty_df.createOrReplaceTempView("counterparty_data");

    dcc_query = f"""SELECT dc.wbu_counterparty_rk wbu_counterparty_rk,
                        MAX (dc.le_counterparty_rk) le_counterparty_rk
                    FROM {scdl_db}.dim_cost_centre dc, var var
                    WHERE     dc.WBU_COUNTERPARTY_RK <> -1
                        AND dc.valid_from_date <= var.v_reporting_date
                        AND dc.valid_to_date > var.v_reporting_date
                GROUP BY dc.wbu_counterparty_rk"""
    dcc = spark.sql(dcc_query);
    dcc.createOrReplaceTempView("dcc");

    print('''\n\n\n********* dcc df loaded *****''')

    stg1_query = f"""SELECT NULL DEAL_ID_PREFIX,
                        fctexp.DEAL_ID,
                        fctexp.VALID_FLAG,
                        'UK-FSA' REPORTING_REGULATOR_CODE,
                        fctexp.UNSETTLED_FACTOR_CODE,
                        fctexp.UNSETTLED_PERIOD_CODE,
                        'A' ADJUST_FLAG,
                        fctexp.REPORTING_TYPE_CODE,
                        fctexp.BANK_BASE_ROLE_CODE,
                        fctexp.ACCRUED_INT_ON_BAL_AMT,
                        fctexp.ACCRUED_INTEREST_GBP,
                        fctexp.APPROVED_APPROACH_CODE,
                        MOD.MODEL_CODE AS PD_MODEL_CODE,
                        dac.REG_EXPOSURE_SUB_TYPE
                        AS b2_irb_asset_class_reg_exposure_sub_type,
                        dac.BASEL_EXPOSURE_TYPE
                        AS b2_irb_asset_class_basel_exposure_type,
                        dac2.BASEL_EXPOSURE_TYPE
                        AS b2_std_asset_class_basel_exposure_type,
                        fctexp.B1_RWA_POST_CRM_AMT,
                        fctexp.B2_APP_ADJUSTED_LGD_RATIO,
                        fctexp.B2_APP_ADJUSTED_PD_RATIO,
                        fctexp.B2_APP_EAD_PRE_CRM_AMT,
                        fctexp.B2_IRB_EAD_PRE_CRM_AMT,
                        fctexp.B2_STD_EAD_PRE_CRM_AMT,
                        fctexp.B2_IRB_EAD_POST_CRM_AMT,
                        fctexp.B2_STD_EAD_POST_CRM_AMT,
                        fctexp.B2_IRB_RWA_POST_CRM_AMT,
                        fctexp.B2_APP_EAD_POST_CRM_AMT,
                        fctexp.B2_STD_RWA_POST_CRM_AMT,
                        fctexp.B2_APP_EXPECTED_LOSS_AMT,
                        fctexp.B2_IRB_NETTED_COLL_GBP,
                        fctexp.B2_APP_RISK_WEIGHT_RATIO,
                        fctexp.B2_APP_RWA_POST_CRM_AMT,
                        fctexp.B2_IRB_DEFAULT_DATE,
                        fctexp.B2_IRB_DRAWN_EAD,
                        fctexp.B2_IRB_DRAWN_EXPECTED_LOSS,
                        fctexp.B2_IRB_DRAWN_RWA,
                        fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS,
                        fctexp.B2_IRB_ORIG_EXP_PRE_CON_FACTOR,
                        fctexp.B2_IRB_UNDRAWN_EAD,
                        fctexp.B2_IRB_UNDRAWN_EXPECTED_LOSS,
                        fctexp.B2_IRB_UNDRAWN_RWA,
                        COALESCE (fctexp.B2_STD_CCF, 0) / 100 B2_STD_CCF,
                        fctexpREC.B2_STD_CCF_LESS_THAN_YEAR,
                        fctexpREC.B2_STD_CCF_MORE_THAN_YEAR,
                        fctexp.B2_STD_CREDIT_QUALITY_STEP,
                        fctexp.B2_STD_DEFAULT_DATE,
                        fctexp.B2_STD_DRAWN_EAD,
                        fctexp.B2_STD_DRAWN_RWA,
                        fctexp.B2_STD_EXP_HAIRCUT_ADJ_AMT,
                        fctexp.SYS_CODE,
                        CASE
                        WHEN COALESCE (fctexp.SYS_CODE, 'X') NOT IN ('ACBCNTNW',
                                                                        'ACBCNTRB',
                                                                        'LNIQNWTR',
                                                                        'LNIQRBTR')
                        THEN
                                COALESCE (fctexp.GROSS_DEP_UTILISATION_AMT,
                                        0)
                            - COALESCE (fctexp.NET_DEP_UTILISATION_AMT, 0)
                        ELSE
                            0
                        END
                        B2_STD_NETTED_COLL_GBP,
                        fctexp.gross_dep_utilisation_amt,
                        fctexp.net_dep_utilisation_amt,
                        fctexp.B2_STD_ORIG_EXP_PRE_CON_FACTOR,
                        fctexp.B2_STD_RISK_WEIGHT_RATIO,
                        fctexp.B2_STD_UNDRAWN_EAD,
                        fctexp.B2_STD_UNDRAWN_RWA,
                        fctexp.B3_CCP_MARGIN_N_INITL_RWA_AMT,
                        fctexp.B3_CCP_MARGIN_INITL_CASH_AMT,
                        fctexp.B3_CCP_MAR_INITL_NON_CASH_AMT,
                        fctexp.B3_CCP_MARGIN_INITL_RWA_AMT,
                        fctexp.B3_CVA_PROVISION_AMT,
                        fctexp.B3_CVA_PROVISION_RESIDUAL_AMT,
                        fctexp.JURISDICTION_FLAG,
                        fctexp.CAPITAL_CUTS,
                        fctexp.BASEL_DATA_FLAG,
                        fctexp.CRR2_FLAG,
                        fctexp.COLL_GOOD_PROV_AMT,
                        fctexp.COMM_LESS_THAN_1_YEAR,
                        fctexp.COMM_MORE_THAN_1_YEAR,
                        fctexp.CVA_CHARGE,
                        fctexp.IFRS9_PROV_IMPAIRMENT_AMT_GBP,
                        fctexp.IFRS9_PROV_WRITE_OFF_AMT_GBP,
                        fctexp.INTERNAL_TRANSACTION_FLAG,
                        fctexp.NET_EXPOSURE,
                        fctexp.NET_EXPOSURE_POST_SEC,
                        fctexp.OFF_BAL_LED_EXP_GBP_POST_SEC,
                        fctexp.OFF_BALANCE_EXPOSURE,
                        fctexp.ON_BAL_LED_EXP_GBP_POST_SEC,
                        fctexp.ON_BALANCE_LEDGER_EXPOSURE,
                        fctexp.PAST_DUE_ASSET_FLAG,
                        fctexp.PROVISION_AMOUNT_GBP,
                        fctexp.RESIDUAL_MATURITY_DAYS,
                        fctexp.RESIDUAL_VALUE,
                        fctexp.RETAIL_OFF_BAL_SHEET_FLAG,
                        fctexp.RISK_ON_GROUP_STRUCTURE,
                        fctexp.RISK_TAKER_GROUP_STRUCTURE,
                        fctexp.SECURITISED_FLAG,
                        fctexp.SET_OFF_INDICATOR,
                        fctexp.SRT_FLAG,
                        fctexp.TRADING_BOOK_FLAG,
                        fctexp.TYPE,
                        fctexp.UNDRAWN_COMMITMENT_AMT,
                        fctexp.UNDRAWN_COMMITMENT_AMT_PST_SEC,
                        fctexp.UNSETTLED_AMOUNT,
                        fctexp.UNSETTLED_PRICE_DIFFERENCE_AMT,
                        fctexp.LEASE_EXPOSURE_GBP,
                        fctexp.LEASE_PAYMENT_VALUE_AMT,
                        fctexp.OBLIGORS_COUNT,
                        fctexp.STD_EXPOSURE_DEFAULT_FLAG,
                        fctexp.CRD4_SOV_SUBSTITUTION_FLAG,
                        fctexp.UNLIKELY_PAY_FLAG,
                        fctexp.CRD4_STD_RP_SME_Flag,
                        fctexp.CRD4_IRB_RP_SME_Flag,
                        fctexp.B3_SME_DISCOUNT_FLAG,
                        fctexp.IRB_SME_DISCOUNT_APP_FLAG,
                        fctexp.TRANSITIONAL_PORTFOLIO_FLAG,
                        fctexp.FULLY_COMPLETE_SEC_FLAG,
                        fctexp.SAF_FILM_LEASING_FLAG,
                        fctexp.PRINCIPAL_AMT,
                        fctexp.MTM,
                        fctexp.AMORTISATION_AMT,
                        fct_count.BSD_MARKER,
                        dim_indust.UK_SIC_CODE,
                        fctexp.IMMOVABLE_PROP_INDICATOR,
                        fctexp.COREP_CRM_REPORTING_FLAG,
                        fctexp.QUAL_REV_EXP_FLAG,
                        fctexp.COMM_REAL_ESTATE_SEC_FLAG,
                        fctexpREC.MORTGAGES_FLAG,
                        fctexp.PERS_ACC_FLAG,
                        fctexp.CCP_RISK_WEIGHT,
                        fctexp.CS_PROXY_USED_FLAG,
                        fctexp.B2_IRB_INTEREST_AT_DEFAULT_GBP,
                        FCTEXP.REIL_AMT_GBP,
                        FCTEXP.REIL_STATUS,
                        fct_count.RP_TURNOVER_MAX_EUR,
                        fct_count.RP_TURNOVER_CCY,
                        fct_count.RP_SALES_TURNOVER_SOURCE,
                        fctexp.B3_SME_RP_FLAG,
                        fctexp.B3_SME_RP_IRB_CORP_FLAG,
                        fct_count.B3_SME_RP_1_5M_FLAG,
                        fct_count.RP_SALES_TURNOVER_LOCAL,
                        fct_count.TOTAL_ASSET_LOCAL,
                        fct_count.LE_TOTAL_ASSET_AMOUNT_EUR,
                        fctexp.MARKET_RISK_SUB_TYPE,
                        fctexp.LE_FI_AVC_CATEGORY,
                        fctexp.B2_IRB_RWA_R_COR_COEFF,
                        fctexp.B2_IRB_RWA_K_CAPITAL_REQ_PRE,
                        fctexp.B2_IRB_RWA_K_CAPITAL_REQ_POST,
                        fctexp.SCALE_UP_COEFENT_OF_CORELATION,
                        fctexp.B3_CVA_CC_ADV_FLAG,
                        dim_rru.LLP_RRU,
                        dim_rru.RRU_RK,
                        fctexp.B3_CVA_CC_ADV_VAR_AVG,
                        fctexp.B3_CVA_CC_ADV_VAR_SPOT,
                        fctexp.B3_CVA_CC_ADV_VAR_STRESS_AVG,
                        fctexp.B3_CVA_CC_ADV_VAR_STRESS_SPOT,
                        fctexp.B3_CVA_CC_CAPITAL_AMT,
                        fctexp.B3_CVA_CC_RWA_AMT,
                        fctexp.B3_CVA_CC_CDS_SNGL_NM_NTNL_AMT,
                        fctexp.B3_CVA_CC_CDS_INDEX_NTNL_AMT,
                        fctexp.B2_ALGO_IRB_CORP_SME_FLAG,
                        fctexp.STATUTORY_LEDGER_BALANCE,
                        fctexp.CARRYING_VALUE,
                        fctexp.LEVERAGE_EXPOSURE,
                        fct_count.STANDALONE_ENTITY,
                        fct_count.RP_TURNOVER_SOURCE_TYPE,
                        fctexp.IFRS9_FINAL_IIS_GBP,
                        fctexp.TOT_REGU_PROV_AMT_GBP,
                        fctexp.NET_MKT_VAL,
                        fctexp.B2_APP_CAP_REQ_POST_CRM_AMT,
                        fctexp.GRDW_POOL_ID,
                        fctexp.NEW_IN_DEFAULT_FLAG,
                        fctexp.RETAIL_POOL_SME_FLAG,
                        fctexp.GRDW_POOL_GROUP_ID,
                        COALESCE (fctexp.B2_IRB_CCF, 0) / 100 B2_IRB_CCF,
                        fctexp.DRAWN_LEDGER_BALANCE,
                        fctexp.E_STAR_GROSS_EXPOSURE,
                        fctexp.B2_IRB_RWA_PRE_CDS,
                        fctexp.LGD_CALC_MODE,
                        fctexp.DEFINITIVE_SLOT_CATEGORY_CALC,
                        fctexp.REPU_COMM_MORE_THAN_YEAR_GBP,
                        fctexp.REPU_COMM_LESS_THAN_YEAR_GBP,
                        fctexp.UNCOMM_UNDRAWN_AMT_GBP,
                        fctexp.TOTAL_UNDRAWN_AMOUNT_GBP,
                        fctexp.COLLECTIVELY_EVALUATED_FLAG,
                        fctexp.ORIGINAL_CURRENCY_CODE,
                        fctexp.ORIGINAL_TRADE_ID,
                        fctexp.E_STAR_NET_EXPOSURE,
                        fctexp.RISK_TAKER_RF_FLAG,
                        fctexp.RISK_TAKER_GS_CODE,
                        fctexp.RISK_ON_RF_FLAG,
                        fctexp.RISK_ON_GS_CODE,
                        fctexp.BORROWER_GS_CODE,
                        fctexp.STS_SECURITISATION,
                        fctexp.STS_SEC_QUAL_CAP_TRTMNT,
                        fctexp.STS_SEC_APPROACH_CODE,
                        fctexp.B3_APP_RWA_INC_CVA_CC_AMT,
                        fctexp.IFRS9_TOT_REGU_ECL_AMT_GBP,
                        fctexp.IFRS9_FINAL_ECL_MES_GBP,
                        fctexp.IFRS9_FINAL_STAGE,
                        fctexp.IFRS9_STAGE_3_TYPE,
                        fctexp.IFRS9_ECL_CURRENCY_CODE,
                        fctexp.IFRS9_TOT_STATUT_ECL_AMT_GBP,
                        fctexp.IFRS9_DISCNT_UNWIND_AMT_GBP,
                        fctexp.B1_EAD_POST_CRM_AMT,
                        fctexp.B1_RISK_WEIGHT_RATIO,
                        fctexp.B1_RWA_PRE_CRM_AMT,
                        fctexp.B1_UNDRAWN_COMMITMENT_GBP,
                        fctexp.B2_IRB_RWA_PRE_CRM_AMT,
                        fctexp.B2_STD_RWA_PRE_CRM_AMT,
                        fctexp.FACILITY_MATCHING_LEVEL,
                        fctexpREC.PRODUCT_FAMILY_CODE,
                        fctexp.CARTHESIS_REPORTING_ID,
                        fctexp.NGAAP_PROVISION_AMT_GBP,
                        dim_count.CIS_CODE AS EXTERNAL_COUNTERPARTY_ID_TEMP,
                        dim_count.RISK_ENTITY_CLASS
                        AS EXTERNAL_COUNTERPARTY_RISK_CLASS,
                        dim_count.QCCP AS External_COUNTERPARTY_QCCP_FLAG,
                        fctexp.SME_DISCOUNT_FACTOR,
                        fctexp.CRR2_501A_DISCOUNT_FLAG,
                        dac.REG_EXPOSURE_TYPE
                        AS B2_IRB_ASSET_CLASS_REG_EXPOSURE_TYPE,
                        dac.basel_exposure_sub_type
                        AS b2_irb_asset_class_basel_exposure_sub_type,
                        dac2.basel_exposure_sub_type
                        AS b2_std_asset_class_basel_exposure_sub_type,
                        div.SUB2 AS DIVISION_SUB2,
                        div.DIVISION AS DIVISION,
                        dprod.REGULATORY_PRODUCT_TYPE
                        AS REGULATORY_PRODUCT_TYPE,
                        dim_wbu_parent.CIS_CODE AS ILE_COUNTERPARTY_CISCODE,
                        dim_wbu_parent.RISK_ENTITY_CLASS AS ILE_RISK_CLASS,
                        dim_parent.CIS_CODE AS BRANCH_CISCODE,
                        dim_parent.RISK_ENTITY_CLASS AS BRANCH_RISK_CLASS,
                        CASE
                        WHEN div.SUB2 IN ('CITIZENS2001',
                                            'CITIZENS2002',
                                            'CITIZENS2003',
                                            'CITIZENS2001_NC',
                                            'CITIZENS2002_NC',
                                            'CITIZENS2003_NC')
                        THEN
                            'Y'
                        ELSE
                            'N'
                        END
                        CITIZENS_OVERRIDE_FLAG,
                        'N' CRD4_REP_STDFLOW_FLAG,
                        CASE
                        WHEN (CASE
                                    WHEN div.SUB2 IN ('CITIZENS2001',
                                                    'CITIZENS2002',
                                                    'CITIZENS2003',
                                                    'CITIZENS2001_NC',
                                                    'CITIZENS2002_NC',
                                                    'CITIZENS2003_NC')
                                    THEN
                                    'Y'
                                    ELSE
                                    'N'
                                END) = 'Y'
                        THEN
                            COALESCE (fctexp.B2_STD_EAD_POST_CRM_AMT, 0)
                        ELSE
                            COALESCE (fctexp.B2_APP_EAD_POST_CRM_AMT, 0)
                        END
                        EAD_POST_CRM_AMT,
                        CASE
                        WHEN     REPORTING_TYPE_CODE = 'NC'
                                AND dac2.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            'Other items'
                        ELSE
                            dac2.BASEL_EXPOSURE_TYPE
                        END
                        COREP_STD_BASEL_EXPOSURE_TYPE,
                        CASE
                        WHEN d_prod_cat.COREP_PRODUCT_CATEGORY_RK = -1
                        THEN
                            'UNKNOWN'
                        ELSE
                            d_prod_cat.COREP_PRODUCT_CATEGORY
                        END
                        AS COREP_PRODUCT_CATEGORY,
                        dim_indust.SECTOR_TYPE AS INDUSTRY_SECTOR_TYPE,
                        DIM_RRU.LL_RRU AS RISK_TAKER_LL_RRU,
                        dim_count.SOVEREIGN_REGIONAL_GOVT_FLAG
                        AS SOVEREIGN_REGIONAL_GOVT_FLAG,
                        MOD2.MODEL_CODE AS EAD_MODEL_CODE,
                        dim_rru2.ll_rru AS RISK_ON_LL_RRU,
                        CASE
                        WHEN     dac3.basel_exposure_type = 'Retail'
                                AND dac3.basel_exposure_sub_type = 'N/A'
                        THEN
                            CASE
                                WHEN fctexp.QUAL_REV_EXP_FLAG = 'Y'
                                THEN
                                    'Qualifying Revolving Exposures'
                                ELSE
                                    CASE
                                    WHEN    fctexp.COMM_REAL_ESTATE_SEC_FLAG =
                                                'Y'
                                            OR fctexpREC.MORTGAGES_FLAG = 'Y'
                                    THEN
                                        CASE
                                            WHEN fctexp.PERS_ACC_FLAG = 'N'
                                            THEN
                                                'Secured by Real Estate Property - SME'
                                            ELSE
                                                'Secured by Real Estate Property - Non-SME'
                                        END
                                    ELSE
                                        CASE
                                            WHEN fctexp.PERS_ACC_FLAG = 'N'
                                            THEN
                                                'Other Retail Exposures - SME'
                                            ELSE
                                                'Other Retail Exposures - Non-SME'
                                        END
                                    END
                            END
                        ELSE
                            dac3.basel_exposure_sub_type
                        END
                        COREP_IRB_EXPOSURE_SUB_TYPE,
                        UPPER (fctexp.ACLM_PRODUCT_TYPE) AS ACLM_PRODUCT_TYPE,
                        d_prod_cat2.ACLM_PRODUCT_TYPE
                        AS ACLM_PRODUCT_TYPE_d_prod_cat2,
                        (SELECT PaRAM_VALUE
                        FROM {scdl_db}.dim_rwa_engine_generic_params
                        WHERE param_code = 'INFRADISCOUNTFACTOR')
                        AS v_infra_discount_factor,
                        CASE
                        WHEN     fctexp.REPORTING_TYPE_CODE = 'NC'
                                AND dac5.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            var.v_oth_itm_asset_class_rk
                        ELSE
                            fctexp.B2_STD_ASSET_CLASS_RK
                        END
                        COREP_STD_ASSET_CLASS_RK,
                        dac.TYPE AS dac_type,
                        dac.valid_flag AS dac_valid_flag,
                        dac.asset_class_rk AS dac_asset_class_rk,
                        dac4.BASEL_EXPOSURE_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE,
                        dce.P3_COUNTRY_DESC AS INCORP_P3_COUNTRY_DESC,
                        dcc2.COST_CENTRE,
                        dce2.P3_COUNTRY_DESC AS OP_P3_COUNTRY_DESC,
                        'EXPOSURE' FLOW_TYPE,
                        COALESCE (fctexp.IFRS9_PROV_WRITE_OFF_AMT_GBP, 0)
                        IMP_WRITE_OFF_AMT,
                        CASE
                        WHEN fctexp.B3_CVA_CC_RWA_AMT IS NOT NULL THEN 'Y'
                        ELSE 'N'
                        END
                        CRD4_REP_CVA_FLAG,
                        IFRS9_BSPL_800_MAP.IAS39_CLASS,
                        CASE
                        WHEN (   dim_count.CIS_CODE = 'Z6VR0IT'
                                OR fctexp.DEAL_ID LIKE 'AR2956%')
                        THEN
                            'OTHER'
                        ELSE
                            'IFRS9'
                        END
                        REP_PROVISION_TYPE,
                        CASE
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA' AND fctexp.DEAL_ID LIKE '%_SI%' THEN 'SI'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA' AND fctexp.DEAL_ID LIKE '%_DTA%' THEN 'DT'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA' AND fctexp.DEAL_ID LIKE '%_NSI%' THEN 'NSI'
                        ELSE
                            NULL
                        END
                        DEAL_TYPE,
                        CASE
                        WHEN fctexp.APPROVED_APPROACH_CODE = 'STD'
                        THEN
                            fctexp.B3_SME_DISCOUNT_FLAG
                        ELSE
                            NULL
                        END
                        STD_SME_DISCOUNT_APP_FLAG,
                        DIM_MATURITY.MATURITY_BAND_DESC,
                        CASE
                        WHEN fctexp.REPORTING_TYPE_CODE = 'SR' THEN 'Y'
                        ELSE 'N'
                        END
                        CRD4_REP_SETTL_FLAG,
                        dim_count.FI_AVC_CATEGORY,
                        d_prod_cat2.COREP_PRODUCT_CATEGORY
                        COREP_EXPOSURE_CATEGORY,
                        pd_band.mgs AS dim_mgs,
                        pd_band.pd_band_code AS dim_pd_band_code,
                        CASE
                        WHEN dim_indust.UK_SIC_CODE = 'UNKNOWN' THEN NULL
                        ELSE dim_indust.UK_SIC_CODE
                        END
                        EC92_CODE,
                        dim_ind.sector_cluster,
                        dim_ind.SUB_SECTOR,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS MEMO_B2_STD_BASEL_EXPOSURE_TYPE,
                        GL_MASTER.BSPL_900_GL_CODE AS BSPL_900_GL_CODE,
                        GL_MASTER.BSPL_60_GL_CODE AS BSPL_60_GL_CODE,
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                                AS rprov_eff_from_year_month
                        FROM {rtd_ref_db}.etl_parameter
                        WHERE parameter_name = 'DE_BBK_IFRS9_PROV_EFF_DATE')
                        AS rprov_eff_from_year_month,
                        dprod.aiml_product_type AS aiml_product_type,
                        dce.P3_COUNTRY_DESC AS INCORP_COUNTRY_DESC,
                        dce2.P3_COUNTRY_DESC AS OP_COUNTRY_DESC,
                        COALESCE (dce.p3_country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_p3_country_desc,
                        COALESCE (dce.country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_country_desc,
                        DIM_RRU.LL_RRU_DESC AS RISK_TAKER_LL_RRU_DESC,
                        DIM_RRU2.LL_RRU_DESC AS RISK_ON_LL_RRU_DESC,
                        dim_counter.cis_code AS LE,
                        dim_count.SYS_CODE AS CSYS_CODE,
                        dim_count.SYS_ID AS CSYS_ID,
                        DIM_IND.SECTOR_TYPE,
                        DIM_IND.SECTOR,
                        dprod.HIGH_PROD,
                        dim_parent.cis_code AS parent_cis_code,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS B2_STD_BASEL_EXPOSURE_TYPE,
                        dim_rule.RULE_DESC,
                        dac4.BASEL_EXPOSURE_SUB_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE,
                        dac4.reg_exposure_type
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE,
                        dac4.TYPE AS B2_STD_PRE_DFLT_ASSET_CLASS_TYPE,
                        dcc2.cost_centre_type,
                        dac5.basel_exposure_sub_type
                        AS MEMO_B2_STD_BASEL_EXPOSURE_SUB_TYPE,
                        dac5.TYPE AS MEMO_B2_STD_TYPE,
                        dac5.reg_exposure_type
                        AS memo_b2_std_reg_exposure_type,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name = 'ICB_EFFECTIVE_DATE') AS INTEGER)
                        AS v_icb_eff_from_year_month_rk,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name =
                                    'CORE_TO_CORE_IG_EXP_EFF_DATE') AS INTEGER)
                        AS v_core_ig_eff_year_month_rk,
                        fctexp.YEAR_MONTH_RK AS YEAR_MONTH,
                        COALESCE (
                        COALESCE (
                            (CASE
                                WHEN dim_wbu_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_wbu_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_count.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_count.counterparty_rk
                            END)),
                        -1)
                        AS LER_CHILD_COUNTERPARTY,
                        fctexp.day_rk,
                        CASE
                        WHEN     fctexp.REPORTING_REGULATOR_CODE = 'CBI'
                                AND fctexp.B2_APP_ADJUSTED_PD_RATIO = 1
                                AND dac.REG_EXPOSURE_SUB_TYPE =
                                    'Income Producing Real Estate'
                                AND dac.BASEL_EXPOSURE_TYPE = 'Corporate'
                                AND fctexp.DEFINITIVE_SLOT_CATEGORY_CALC =
                                    'N'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN dprod.regulatory_product_type = 'PR_EQ_CIU'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN MOD.model_code = 'SRW'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN MOD.model_code = 'SG'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Object Finance',
                                                                'Commodities Finance')
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Income Producing Real Estate',
                                                                'Project Finance')
                        THEN
                            'AIRB - SL Slotting Approach'
                        ELSE
                            'AIRB - PD/LGD Approach'
                        END
                        RWA_CALC_METHOD,
                        fctexp.B2_IRB_ASSET_CLASS_RK,
                        fctexp.rru_rk AS risk_taker_rru,
                        fctexp.risk_on_rru_rk AS risk_on_rru,
                        CASE
                        WHEN COALESCE (fctexp.B2_APP_EAD_POST_CRM_AMT, 0) =
                                0
                        THEN
                            'NULL EAD'
                        ELSE
                            CASE
                                WHEN fctexp.COUNTERPARTY_RK = -1
                                THEN
                                    'RETAIL'
                                ELSE
                                    'CORP'
                            END
                        END
                        OBLIGOR_TYPE,
                        div.SUB1,
                        div.SUB1_DESC,
                        div.SUB2,
                        div.SUB2_DESC,
                        div.SUB3,
                        div.SUB3_DESC,
                        div.SUB4,
                        div.SUB4_DESC,
                        div.SUB5,
                        div.SUB5_DESC,
                        div.DIVISION_DESC
                FROM {scdl_db}.FCT_EXPOSURES fctexp
                        JOIN VAR ON VAR.rYear_Month_RK = fctexp.YEAR_MONTH_RK
                        LEFT OUTER JOIN {scdl_db}.FCT_EXPOSURES_REC fctexpREC
                        ON (    fctexp.DEAL_ID = fctexpREC.DEAL_ID
                            AND fctexp.DAY_RK = fctexpREC.DAY_RK
                            AND fctexp.YEAR_MONTH_RK =
                                    fctexpREC.YEAR_MONTH_RK
                            AND fctexp.VALID_FLAG = fctexpREC.VALID_FLAG)
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU DIM_RRU
                        ON (    fctexp.rru_rk = dim_rru.rru_rk
                            AND DIM_RRU.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_RRU.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat
                        ON ( (CASE
                                    WHEN UPPER (fctexp.ACLM_PRODUCT_TYPE) =
                                            'N/A'
                                    THEN
                                    'UNKNOWN'
                                    ELSE
                                    UPPER (fctexp.ACLM_PRODUCT_TYPE)
                                END) = d_prod_cat.ACLM_PRODUCT_TYPE)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES fct_count
                        ON (    fctexp.COUNTERPARTY_RK =
                                    fct_count.COUNTERPARTY_RK
                            AND fctexp.DAY_RK = fct_count.DAY_RK
                            AND fctexp.YEAR_MONTH_RK =
                                    fct_count.YEAR_MONTH_RK
                            AND fct_count.VALID_FLAG = 'Y')
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY dim_indust
                        ON (    fct_count.EC92_INDUSTRY_RK =
                                    dim_indust.INDUSTRY_RK
                            AND fct_count.VALID_FLAG = 'Y'
                            AND dim_indust.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_indust.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac
                        ON (    fctexp.B2_IRB_ASSET_CLASS_RK =
                                    dac.ASSET_CLASS_RK
                            AND dac.valid_from_date <= var.v_reporting_date
                            AND dac.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac2
                        ON (    fctexp.B2_STD_ASSET_CLASS_RK =
                                    dac2.ASSET_CLASS_RK
                            AND dac2.TYPE = 'B2_STD'
                            AND dac2.BASEL_EXPOSURE_TYPE = 'Other items'
                            AND dac2.valid_from_date <= var.v_reporting_date
                            AND dac2.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_DIVISION div
                        ON (    fctexp.DIVISION_RK = div.DIVISION_RK
                            AND div.valid_from_date <= var.v_reporting_date
                            AND div.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL MOD
                        ON (    fctexp.PD_MODEL_RK = MOD.MODEL_RK
                            AND MOD.valid_from_date <= var.v_reporting_date
                            AND MOD.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_PRODUCT dprod
                        ON (    fctexp.PRODUCT_RK = dprod.PRODUCT_RK
                            AND dprod.valid_from_date <=
                                    var.v_reporting_date
                            AND dprod.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.dim_cost_centre dc
                        ON (    fctexp.COST_CENTRE_RK = dc.COST_CENTRE_RK
                            AND dc.valid_from_date <= var.v_reporting_date
                            AND dc.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN counterparty_data dim_count
                        ON (    fctexp.COUNTERPARTY_RK =
                                    dim_count.COUNTERPARTY_RK
                            AND dim_count.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_count.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN dcc dcc1
                        ON (dim_count.counterparty_rk =
                                dcc1.wbu_counterparty_rk)
                        LEFT JOIN counterparty_data dim_wbu_parent
                        ON (    dim_wbu_parent.counterparty_rk =
                                    dcc1.le_counterparty_rk
                            AND dim_wbu_parent.group_internal_flag = 'Y'
                            AND dim_wbu_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_wbu_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT JOIN counterparty_data dim_parent
                        ON (    dim_parent.counterparty_rk =
                                    fct_count.branch_parent_counterparty_rk
                            AND dim_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL MOD2
                        ON     fctexp.EAD_MODEL_RK = MOD2.MODEL_RK
                            AND MOD2.valid_from_date <= var.v_reporting_date
                            AND MOD2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU DIM_RRU2
                        ON     fctexp.RISK_ON_RRU_RK = dim_rru2.rru_rk
                            AND DIM_RRU2.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_RRU2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac3
                        ON     fctexp.COREP_ASSET_CLASS_RK =
                                    dac3.ASSET_CLASS_RK
                            AND dac3.valid_from_date <= var.v_reporting_date
                            AND dac3.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN
                        {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat2
                        ON fctexp.COREP_PRODUCT_CATEGORY_RK =
                                d_prod_cat2.COREP_PRODUCT_CATEGORY_RK
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac4
                        ON     fctexp.B2_STD_PRE_DFLT_ASSET_CLASS_RK =
                                    dac4.ASSET_CLASS_RK
                            AND dac4.valid_from_date <= var.v_reporting_date
                            AND dac4.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce
                        ON     fctexp.INCORP_COUNTRY_RK = dce.COUNTRY_RK
                            AND dce.valid_from_date <= var.v_reporting_date
                            AND dce.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_COST_CENTRE dcc2
                        ON (    fctexp.COST_CENTRE_RK =
                                    dcc2.COST_CENTRE_RK
                            AND dcc2.valid_flag = 'Y'
                            AND dcc2.valid_from_date <= var.v_reporting_date
                            AND dcc2.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce2
                        ON     fctexp.OP_COUNTRY_RK = dce2.COUNTRY_RK
                            AND dce2.valid_from_date <= var.v_reporting_date
                            AND dce2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.GL_MASTER GL_MASTER
                        ON (    fctexp.GL_ACCOUNT_RK = GL_MASTER.GL_RK
                            AND GL_MASTER.valid_from_date <=
                                    var.v_reporting_date
                            AND GL_MASTER.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.IFRS9_BSPL_800_MAP IFRS9_BSPL_800_MAP
                        ON (GL_MASTER.BSPL_800_GL_CODE =
                                IFRS9_BSPL_800_MAP.BSPL800_CODE)
                        LEFT OUTER JOIN {scdl_db}.DIM_MATURITY_BAND DIM_MATURITY
                        ON (    (   (    (fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS) >
                                            DIM_MATURITY.LOWER_VALUE
                                        AND (fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS) <
                                            DIM_MATURITY.UPPER_VALUE)
                                    OR (    fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.LOWER_VALUE
                                        AND DIM_MATURITY.LOWER_VAL_INCLUSIVE =
                                            'Y')
                                    OR (    fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.UPPER_VALUE
                                        AND DIM_MATURITY.UPPER_VAL_INCLUSIVE =
                                            'Y'))
                            AND DIM_MATURITY.TYPE = 'S'
                            AND DIM_MATURITY.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_MATURITY.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_PD_BAND pd_band
                        ON (    (   (    COALESCE (
                                            fctexp.b2_app_adjusted_pd_ratio,
                                            0.02501) > pd_band.lower_value
                                        AND COALESCE (
                                            fctexp.b2_app_adjusted_pd_ratio,
                                            0.02501) < pd_band.upper_value)
                                    OR (    COALESCE (
                                            fctexp.b2_app_adjusted_pd_ratio,
                                            0.02501) = pd_band.lower_value
                                        AND pd_band.lower_val_inclusive = 'Y')
                                    OR (    COALESCE (
                                            fctexp.b2_app_adjusted_pd_ratio,
                                            0.02501) = pd_band.upper_value
                                        AND pd_band.upper_val_inclusive = 'Y'))
                            AND pd_band.TYPE = 'MGS'
                            AND pd_band.valid_from_date <=
                                    var.v_reporting_date
                            AND pd_band.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY DIM_IND
                        ON (    fctexp.INDUSTRY_RK = DIM_IND.INDUSTRY_RK
                            AND DIM_IND.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_IND.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac5
                        ON     fctexp.B2_STD_ASSET_CLASS_RK =
                                    dac5.ASSET_CLASS_RK
                            AND dac5.valid_from_date <= var.v_reporting_date
                            AND dac5.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN counterparty_data dim_counter
                        ON     dcc2.LE_COUNTERPARTY_RK =
                                    dim_counter.counterparty_rk
                            AND dim_counter.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_counter.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_op_country
                        ON     fctexp.OP_COUNTRY_RK =
                                    d_op_country.COUNTRY_RK
                            AND d_op_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_op_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_incorp_country
                        ON     fctexp.INCORP_COUNTRY_RK =
                                    d_incorp_country.COUNTRY_RK
                            AND d_incorp_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_incorp_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RULE dim_rule
                        ON fctexp.STD_RW_PRIORITY_ORDER_RULE_RK =
                                dim_rule.RULE_RK
                WHERE     fctexp.DAY_RK = var.rDay_rk
                        AND fctexp.YEAR_MONTH_RK = var.rYear_Month_RK
                        AND (    (fctexp.VALID_FLAG = 'Y')
                            AND (   (fctexp.ADJUST_FLAG = 'U')
                                OR (fctexp.ADJUST_FLAG = 'A')))
                        AND (CASE
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND fctexp.ACLM_PRODUCT_TYPE IN ('BASEL-OTC',
                                                                    'REPO')
                                THEN
                                CASE
                                    WHEN     fctexp.SYS_CODE IN ('SABFUKOR',
                                                                'MUREUKOR',
                                                                'MTH_OTH',
                                                                'DAY_OTH')
                                        AND fctexp.CAPITAL_CUTS = '4'
                                        AND JURISDICTION_FLAG = 'UK'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexp.SYS_CODE,
                                                        'XX') NOT IN ('SABFUKOR',
                                                                    'MUREUKOR',
                                                                    'MTH_OTH',
                                                                    'DAY_OTH')
                                        AND (   fctexp.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexp.CAPITAL_CUTS = ''
                                                OR LENGTH (
                                                    fctexp.CAPITAL_CUTS) =
                                                    0)
                                        AND (   fctexp.JURISDICTION_FLAG
                                                    IS NULL
                                                OR fctexp.JURISDICTION_FLAG =
                                                    ''
                                                OR LENGTH (
                                                    fctexp.JURISDICTION_FLAG) =
                                                    0)
                                    THEN
                                        'TRUE'
                                END
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND COALESCE (fctexp.ACLM_PRODUCT_TYPE,
                                                'XX') NOT IN ('BASEL-OTC',
                                                                'REPO')
                                THEN
                                'TRUE'
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rdualrep_eff_from_yrmnth_rk
                                    AND VAR.rYear_Month_RK <
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND COALESCE (fctexp.CRR2_FLAG, 'N') <>
                                            'Y'
                                    AND (   fctexp.CAPITAL_CUTS IS NULL
                                        OR fctexp.CAPITAL_CUTS = ''
                                        OR LENGTH (fctexp.CAPITAL_CUTS) = 0)
                                THEN
                                'TRUE'
                                WHEN VAR.rYear_Month_RK <
                                        VAR.rdualrep_eff_from_yrmnth_rk
                                THEN
                                'TRUE'
                            END) = 'TRUE'
                UNION ALL
                SELECT fctexpDNB.DEAL_ID_PREFIX,
                        fctexpDNB.DEAL_ID_PREFIX || fctexpDNB.DEAL_ID DEAL_ID,
                        fctexpDNB.VALID_FLAG,
                        'NL-DNB' REPORTING_REGULATOR_CODE,
                        fctexpDNB.UNSETTLED_FACTOR_CODE,
                        fctexpDNB.UNSETTLED_PERIOD_CODE,
                        'A' ADJUST_FLAG,
                        fctexpDNB.REPORTING_TYPE_CODE,
                        fctexpDNB.BANK_BASE_ROLE_CODE,
                        fctexpDNB.ACCRUED_INT_ON_BAL_AMT,
                        fctexpDNB.ACCRUED_INTEREST_GBP,
                        fctexpDNB.APPROVED_APPROACH_CODE,
                        DIM_MODEL.MODEL_CODE AS PD_MODEL_CODE,
                        dac.REG_EXPOSURE_SUB_TYPE
                        AS B2_IRB_ASSET_CLASS_REG_EXPOSURE_SUB_TYPE,
                        dac.BASEL_EXPOSURE_TYPE
                        AS b2_irb_asset_class_basel_exposure_type,
                        dac2.BASEL_EXPOSURE_TYPE
                        AS b2_std_asset_class_basel_exposure_type,
                        fctexpDNB.B1_RWA_POST_CRM_AMT,
                        fctexpDNB.B2_APP_ADJUSTED_LGD_RATIO,
                        fctexpDNB.B2_APP_ADJUSTED_PD_RATIO,
                        fctexpDNB.B2_APP_EAD_PRE_CRM_AMT,
                        fctexpDNB.B2_IRB_EAD_PRE_CRM_AMT,
                        fctexpDNB.B2_STD_EAD_PRE_CRM_AMT,
                        fctexpDNB.B2_IRB_EAD_POST_CRM_AMT,
                        fctexpDNB.B2_STD_EAD_POST_CRM_AMT,
                        fctexpDNB.B2_IRB_RWA_POST_CRM_AMT,
                        fctexpDNB.B2_APP_EAD_POST_CRM_AMT,
                        fctexpDNB.B2_STD_RWA_POST_CRM_AMT,
                        fctexpDNB.B2_APP_EXPECTED_LOSS_AMT,
                        fctexpDNB.B2_IRB_NETTED_COLL_GBP,
                        fctexpDNB.B2_APP_RISK_WEIGHT_RATIO,
                        CASE  
                        WHEN DIM_MODEL.MODEL_CODE = 'LC'
                            OR (dac.REG_EXPOSURE_SUB_TYPE IN ('Income Producing Real Estate',
                                                                'Project Finance')
                            and dac.BASEL_EXPOSURE_TYPE = 'Corporate')
                        THEN fctexpDNB.B2_IRB_RWA_POST_CRM_AMT
                        ELSE fctexpDNB.B2_STD_RWA_POST_CRM_AMT
                        END B2_APP_RWA_POST_CRM_AMT,
                        fctexpDNB.B2_IRB_DEFAULT_DATE,
                        fctexpDNB.B2_IRB_DRAWN_EAD,
                        fctexpDNB.B2_IRB_DRAWN_EXPECTED_LOSS,
                        fctexpDNB.B2_IRB_DRAWN_RWA,
                        fctexpDNB.B2_IRB_EFFECTIVE_MATURITY_YRS,
                        fctexpDNB.B2_IRB_ORIG_EXP_PRE_CON_FACTOR,
                        fctexpDNB.B2_IRB_UNDRAWN_EAD,
                        fctexpDNB.B2_IRB_UNDRAWN_EXPECTED_LOSS,
                        fctexpDNB.B2_IRB_UNDRAWN_RWA,
                        COALESCE (fctexpDNB.B2_STD_CCF, 0) / 100 B2_STD_CCF,
                        fctexpREC.B2_STD_CCF_LESS_THAN_YEAR,
                        fctexpREC.B2_STD_CCF_MORE_THAN_YEAR,
                        fctexpDNB.B2_STD_CREDIT_QUALITY_STEP,
                        fctexpDNB.B2_STD_DEFAULT_DATE,
                        fctexpDNB.B2_STD_DRAWN_EAD,
                        fctexpDNB.B2_STD_DRAWN_RWA,
                        fctexpDNB.B2_STD_EXP_HAIRCUT_ADJ_AMT,
                        fctexpDNB.SYS_CODE,
                        CASE
                        WHEN COALESCE (fctexpDNB.SYS_CODE, 'X') NOT IN ('ACBCNTNW',
                                                                        'ACBCNTRB',
                                                                        'LNIQNWTR',
                                                                        'LNIQRBTR')
                        THEN
                                COALESCE (
                                fctexpDNB.GROSS_DEP_UTILISATION_AMT,
                                0)
                            - COALESCE (fctexpDNB.NET_DEP_UTILISATION_AMT,
                                        0)
                        ELSE
                            0
                        END
                        B2_STD_NETTED_COLL_GBP,
                        fctexpDNB.gross_dep_utilisation_amt,
                        fctexpDNB.net_dep_utilisation_amt,
                        fctexpDNB.B2_STD_ORIG_EXP_PRE_CON_FACTOR,
                        fctexpDNB.B2_STD_RISK_WEIGHT_RATIO,
                        fctexpDNB.B2_STD_UNDRAWN_EAD,
                        fctexpDNB.B2_STD_UNDRAWN_RWA,
                        fctexpDNB.B3_CCP_MARGIN_N_INITL_RWA_AMT,
                        fctexpDNB.B3_CCP_MARGIN_INITL_CASH_AMT,
                        fctexpDNB.B3_CCP_MAR_INITL_NON_CASH_AMT,
                        fctexpDNB.B3_CCP_MARGIN_INITL_RWA_AMT,
                        fctexpDNB.B3_CVA_PROVISION_AMT,
                        fctexpDNB.B3_CVA_PROVISION_RESIDUAL_AMT,
                        fctexpDNB.JURISDICTION_FLAG,
                        fctexpDNB.CAPITAL_CUTS,
                        CASE
                        WHEN     var.rYear_Month_RK >=
                                    var.rpradiv_eff_from_yrmnth_rk
                                AND fctexpDNB.ACLM_PRODUCT_TYPE IN ('REPO',
                                                                    'BASEL-OTC')
                        THEN
                            CASE
                                WHEN     fctexpDNB.SYS_CODE IN ('SABFI2OR',
                                                                'MUREX2OR',
                                                                'MTH_OTH',
                                                                'DAY_OTH')
                                    AND fctexpDNB.JURISDICTION_FLAG = 'EU'
                                    AND fctexpDNB.CAPITAL_CUTS = '4'
                                THEN
                                    'Y'
                                ELSE
                                    fctexpDNB.BASEL_DATA_FLAG
                            END
                        WHEN     var.rYear_Month_RK <
                                    var.rpradiv_eff_from_yrmnth_rk
                                AND var.rYear_Month_RK >=
                                    var.rdualrep_eff_from_yrmnth_rk
                        THEN
                            CASE
                                WHEN     fctexpDNB.SYS_CODE IN ('SABFI2OR',
                                                                'MUREX2OR')
                                    AND fctexpDNB.CRR2_FLAG = 'Y'
                                    AND fctexpDNB.CAPITAL_CUTS = '4'
                                THEN
                                    'Y'
                                ELSE
                                    fctexpDNB.BASEL_DATA_FLAG
                            END
                        ELSE
                            fctexpDNB.BASEL_DATA_FLAG
                        END
                        BASEL_DATA_FLAG,
                        --fctexpDNB.BASEL_DATA_FLAG,
                        fctexpDNB.CRR2_FLAG,
                        fctexpDNB.COLL_GOOD_PROV_AMT,
                        fctexpDNB.COMM_LESS_THAN_1_YEAR,
                        fctexpDNB.COMM_MORE_THAN_1_YEAR,
                        fctexpDNB.CVA_CHARGE,
                        fctexpDNB.IFRS9_PROV_IMPAIRMENT_AMT_GBP,
                        fctexpDNB.IFRS9_PROV_WRITE_OFF_AMT_GBP,
                        fctexpDNB.INTERNAL_TRANSACTION_FLAG,
                        fctexpDNB.NET_EXPOSURE,
                        fctexpDNB.NET_EXPOSURE_POST_SEC,
                        fctexpDNB.OFF_BAL_LED_EXP_GBP_POST_SEC,
                        fctexpDNB.OFF_BALANCE_EXPOSURE,
                        fctexpDNB.ON_BAL_LED_EXP_GBP_POST_SEC,
                        fctexpDNB.ON_BALANCE_LEDGER_EXPOSURE,
                        fctexpDNB.PAST_DUE_ASSET_FLAG,
                        fctexpDNB.PROVISION_AMOUNT_GBP,
                        fctexpDNB.RESIDUAL_MATURITY_DAYS,
                        fctexpDNB.RESIDUAL_VALUE,
                        fctexpDNB.RETAIL_OFF_BAL_SHEET_FLAG,
                        fctexpDNB.RISK_ON_GROUP_STRUCTURE,
                        fctexpDNB.RISK_TAKER_GROUP_STRUCTURE,
                        fctexpDNB.SECURITISED_FLAG,
                        fctexpDNB.SET_OFF_INDICATOR,
                        fctexpDNB.SRT_FLAG,
                        fctexpDNB.TRADING_BOOK_FLAG,
                        fctexpDNB.TYPE,
                        fctexpDNB.UNDRAWN_COMMITMENT_AMT,
                        fctexpDNB.UNDRAWN_COMMITMENT_AMT_PST_SEC,
                        fctexpDNB.UNSETTLED_AMOUNT,
                        fctexpDNB.UNSETTLED_PRICE_DIFFERENCE_AMT,
                        fctexpDNB.LEASE_EXPOSURE_GBP,
                        fctexpDNB.LEASE_PAYMENT_VALUE_AMT,
                        fctexpDNB.OBLIGORS_COUNT,
                        fctexpDNB.STD_EXPOSURE_DEFAULT_FLAG,
                        fctexpDNB.CRD4_SOV_SUBSTITUTION_FLAG,
                        fctexpDNB.UNLIKELY_PAY_FLAG,
                        fctexpDNB.CRD4_STD_RP_SME_Flag,
                        fctexpDNB.CRD4_IRB_RP_SME_Flag,
                        fctexpDNB.B3_SME_DISCOUNT_FLAG,
                        fctexpDNB.IRB_SME_DISCOUNT_APP_FLAG,
                        fctexpDNB.TRANSITIONAL_PORTFOLIO_FLAG,
                        fctexpDNB.FULLY_COMPLETE_SEC_FLAG,
                        fctexpDNB.SAF_FILM_LEASING_FLAG,
                        fctexpDNB.PRINCIPAL_AMT,
                        fctexpDNB.MTM,
                        fctexpDNB.AMORTISATION_AMT,
                        fct_count.BSD_MARKER,
                        dim_indust.UK_SIC_CODE,
                        fctexpDNB.IMMOVABLE_PROP_INDICATOR,
                        fctexpDNB.COREP_CRM_REPORTING_FLAG,
                        fctexpDNB.QUAL_REV_EXP_FLAG,
                        fctexpDNB.COMM_REAL_ESTATE_SEC_FLAG,
                        fctexpREC.MORTGAGES_FLAG,
                        fctexpDNB.PERS_ACC_FLAG,
                        fctexpDNB.CCP_RISK_WEIGHT,
                        fctexpDNB.CS_PROXY_USED_FLAG,
                        FCTEXPDNB.B2_IRB_INTEREST_AT_DEFAULT_GBP,
                        fctexpDNB.REIL_AMT_GBP,
                        fctexpDNB.REIL_STATUS,
                        fct_count.RP_TURNOVER_MAX_EUR,
                        fct_count.RP_TURNOVER_CCY,
                        fct_count.RP_SALES_TURNOVER_SOURCE,
                        fctexpDNB.B3_SME_RP_FLAG,
                        fctexpDNB.B3_SME_RP_IRB_CORP_FLAG,
                        fct_count.B3_SME_RP_1_5M_FLAG,
                        fct_count.RP_SALES_TURNOVER_LOCAL,
                        fct_count.TOTAL_ASSET_LOCAL,
                        fct_count.LE_TOTAL_ASSET_AMOUNT_EUR,
                        fctexpDNB.MARKET_RISK_SUB_TYPE,
                        fctexpDNB.LE_FI_AVC_CATEGORY,
                        fctexpDNB.B2_IRB_RWA_R_COR_COEFF,
                        fctexpDNB.B2_IRB_RWA_K_CAPITAL_REQ_PRE,
                        fctexpDNB.B2_IRB_RWA_K_CAPITAL_REQ_POST,
                        fctexpDNB.SCALE_UP_COEFENT_OF_CORELATION,
                        fctexpDNB.B3_CVA_CC_ADV_FLAG,
                        dim_rru.LLP_RRU,
                        dim_rru.RRU_RK,
                        fctexpDNB.B3_CVA_CC_ADV_VAR_AVG,
                        fctexpDNB.B3_CVA_CC_ADV_VAR_SPOT,
                        fctexpDNB.B3_CVA_CC_ADV_VAR_STRESS_AVG,
                        fctexpDNB.B3_CVA_CC_ADV_VAR_STRESS_SPOT,
                        fctexpDNB.B3_CVA_CC_CAPITAL_AMT,
                        fctexpDNB.B3_CVA_CC_RWA_AMT,
                        fctexpDNB.B3_CVA_CC_CDS_SNGL_NM_NTNL_AMT,
                        fctexpDNB.B3_CVA_CC_CDS_INDEX_NTNL_AMT,
                        fctexpDNB.B2_ALGO_IRB_CORP_SME_FLAG,
                        fctexpDNB.STATUTORY_LEDGER_BALANCE,
                        fctexpDNB.CARRYING_VALUE,
                        fctexpDNB.LEVERAGE_EXPOSURE,
                        fct_count.STANDALONE_ENTITY,
                        fct_count.RP_TURNOVER_SOURCE_TYPE,
                        fctexpDNB.IFRS9_FINAL_IIS_GBP,
                        fctexpDNB.TOT_REGU_PROV_AMT_GBP,
                        fctexpDNB.NET_MKT_VAL,
                        fctexpDNB.B2_APP_CAP_REQ_POST_CRM_AMT,
                        fctexpDNB.GRDW_POOL_ID,
                        fctexpDNB.NEW_IN_DEFAULT_FLAG,
                        fctexpDNB.RETAIL_POOL_SME_FLAG,
                        fctexpDNB.GRDW_POOL_GROUP_ID,
                        COALESCE (fctexpDNB.B2_IRB_CCF, 0) / 100 B2_IRB_CCF,
                        fctexpDNB.DRAWN_LEDGER_BALANCE,
                        fctexpDNB.E_STAR_GROSS_EXPOSURE,
                        fctexpDNB.B2_IRB_RWA_PRE_CDS,
                        fctexpDNB.LGD_CALC_MODE,
                        fctexpDNB.DEFINITIVE_SLOT_CATEGORY_CALC,
                        fctexpDNB.REPU_COMM_MORE_THAN_YEAR_GBP,
                        fctexpDNB.REPU_COMM_LESS_THAN_YEAR_GBP,
                        fctexpDNB.UNCOMM_UNDRAWN_AMT_GBP,
                        fctexpDNB.TOTAL_UNDRAWN_AMOUNT_GBP,
                        fctexpDNB.COLLECTIVELY_EVALUATED_FLAG,
                        fctexpDNB.ORIGINAL_CURRENCY_CODE,
                        fctexpDNB.ORIGINAL_TRADE_ID,
                        fctexpDNB.E_STAR_NET_EXPOSURE,
                        fctexpDNB.RISK_TAKER_RF_FLAG,
                        fctexpDNB.RISK_TAKER_GS_CODE,
                        fctexpDNB.RISK_ON_RF_FLAG,
                        fctexpDNB.RISK_ON_GS_CODE,
                        fctexpDNB.BORROWER_GS_CODE,
                        fctexpDNB.STS_SECURITISATION,
                        fctexpDNB.STS_SEC_QUAL_CAP_TRTMNT,
                        fctexpDNB.STS_SEC_APPROACH_CODE,
                        fctexpDNB.B3_APP_RWA_INC_CVA_CC_AMT,
                        fctexpDNB.IFRS9_TOT_REGU_ECL_AMT_GBP,
                        fctexpDNB.IFRS9_FINAL_ECL_MES_GBP,
                        fctexpDNB.IFRS9_FINAL_STAGE,
                        fctexpDNB.IFRS9_STAGE_3_TYPE,
                        fctexpDNB.IFRS9_ECL_CURRENCY_CODE,
                        fctexpDNB.IFRS9_TOT_STATUT_ECL_AMT_GBP,
                        fctexpDNB.IFRS9_DISCNT_UNWIND_AMT_GBP,
                        fctexpDNB.B1_EAD_POST_CRM_AMT,
                        fctexpDNB.B1_RISK_WEIGHT_RATIO,
                        fctexpDNB.B1_RWA_PRE_CRM_AMT,
                        fctexpDNB.B1_UNDRAWN_COMMITMENT_GBP,
                        fctexpDNB.B2_IRB_RWA_PRE_CRM_AMT,
                        fctexpDNB.B2_STD_RWA_PRE_CRM_AMT,
                        fctexpDNB.FACILITY_MATCHING_LEVEL,
                        fctexpREC.PRODUCT_FAMILY_CODE,
                        fctexpDNB.CARTHESIS_REPORTING_ID,
                        fctexpDNB.NGAAP_PROVISION_AMT_GBP,
                        dim_count.CIS_CODE AS EXTERNAL_COUNTERPARTY_ID_TEMP,
                        dim_count.RISK_ENTITY_CLASS
                        AS EXTERNAL_COUNTERPARTY_RISK_CLASS,
                        dim_count.QCCP AS External_COUNTERPARTY_QCCP_FLAG,
                        fctexpDNB.SME_DISCOUNT_FACTOR,
                        fctexpDNB.CRR2_501A_DISCOUNT_FLAG,
                        dac.REG_EXPOSURE_TYPE
                        AS b2_irb_asset_class_reg_exposure_type,
                        dac.basel_exposure_sub_type
                        AS b2_irb_asset_class_basel_exposure_sub_type,
                        dac2.basel_exposure_sub_type
                        AS b2_std_asset_class_basel_exposure_sub_type,
                        div.SUB2 AS DIVISION_SUB2,
                        div.DIVISION AS DIVISION,
                        dprod.REGULATORY_PRODUCT_TYPE
                        AS REGULATORY_PRODUCT_TYPE,
                        dim_wbu_parent.CIS_CODE AS ILE_COUNTERPARTY_CISCODE,
                        dim_wbu_parent.RISK_ENTITY_CLASS AS ILE_RISK_CLASS,
                        dim_parent.CIS_CODE AS BRANCH_CISCODE,
                        dim_parent.RISK_ENTITY_CLASS AS BRANCH_RISK_CLASS,
                        CASE
                        WHEN div.SUB2 IN ('CITIZENS2001',
                                            'CITIZENS2002',
                                            'CITIZENS2003',
                                            'CITIZENS2001_NC',
                                            'CITIZENS2002_NC',
                                            'CITIZENS2003_NC')
                        THEN
                            'Y'
                        ELSE
                            'N'
                        END
                        CITIZENS_OVERRIDE_FLAG,
                        'N' CRD4_REP_STDFLOW_FLAG,
                        CASE
                        WHEN (CASE
                                    WHEN div.SUB2 IN ('CITIZENS2001',
                                                    'CITIZENS2002',
                                                    'CITIZENS2003',
                                                    'CITIZENS2001_NC',
                                                    'CITIZENS2002_NC',
                                                    'CITIZENS2003_NC')
                                    THEN
                                    'Y'
                                    ELSE
                                    'N'
                                END) = 'Y'
                        THEN
                            COALESCE (fctexpDNB.B2_STD_EAD_POST_CRM_AMT, 0)
                        ELSE
                            COALESCE (fctexpDNB.B2_APP_EAD_POST_CRM_AMT, 0)
                        END
                        EAD_POST_CRM_AMT,
                        CASE
                        WHEN     REPORTING_TYPE_CODE = 'NC'
                                AND dac2.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            'Other items'
                        ELSE
                            dac2.BASEL_EXPOSURE_TYPE
                        END
                        COREP_STD_BASEL_EXPOSURE_TYPE,
                        CASE
                        WHEN d_prod_cat.COREP_PRODUCT_CATEGORY_RK = -1
                        THEN
                            'UNKNOWN'
                        ELSE
                            d_prod_cat.COREP_PRODUCT_CATEGORY
                        END
                        AS COREP_PRODUCT_CATEGORY,
                        dim_indust.SECTOR_TYPE AS INDUSTRY_SECTOR_TYPE,
                        DIM_RRU.LL_RRU AS RISK_TAKER_LL_RRU,
                        dim_count.SOVEREIGN_REGIONAL_GOVT_FLAG
                        AS SOVEREIGN_REGIONAL_GOVT_FLAG,
                        MOD2.MODEL_CODE AS EAD_MODEL_CODE,
                        dim_rru2.ll_rru AS RISK_ON_LL_RRU,
                        CASE
                        WHEN     dac3.basel_exposure_type = 'Retail'
                                AND dac3.basel_exposure_sub_type = 'N/A'
                        THEN
                            CASE
                                WHEN fctexpDNB.QUAL_REV_EXP_FLAG = 'Y'
                                THEN
                                    'Qualifying Revolving Exposures'
                                ELSE
                                    CASE
                                    WHEN    fctexpDNB.COMM_REAL_ESTATE_SEC_FLAG =
                                                'Y'
                                            OR fctexpREC.MORTGAGES_FLAG = 'Y'
                                    THEN
                                        CASE
                                            WHEN fctexpDNB.PERS_ACC_FLAG =
                                                    'N'
                                            THEN
                                                'Secured by Real Estate Property - SME'
                                            ELSE
                                                'Secured by Real Estate Property - Non-SME'
                                        END
                                    ELSE
                                        CASE
                                            WHEN fctexpDNB.PERS_ACC_FLAG =
                                                    'N'
                                            THEN
                                                'Other Retail Exposures - SME'
                                            ELSE
                                                'Other Retail Exposures - Non-SME'
                                        END
                                    END
                            END
                        ELSE
                            dac3.basel_exposure_sub_type
                        END
                        COREP_IRB_EXPOSURE_SUB_TYPE,
                        UPPER (fctexpDNB.ACLM_PRODUCT_TYPE)
                        AS ACLM_PRODUCT_TYPE,
                        d_prod_cat2.ACLM_PRODUCT_TYPE
                        AS ACLM_PRODUCT_TYPE_d_prod_cat2,
                        (SELECT PaRAM_VALUE
                        FROM {scdl_db}.dim_rwa_engine_generic_params
                        WHERE param_code = 'INFRADISCOUNTFACTOR')
                        AS v_infra_discount_factor,
                        CASE
                        WHEN     fctexpDNB.REPORTING_TYPE_CODE = 'NC'
                                AND dac5.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            var.v_oth_itm_asset_class_rk
                        ELSE
                            fctexpDNB.B2_STD_ASSET_CLASS_RK
                        END
                        COREP_STD_ASSET_CLASS_RK,
                        dac.TYPE AS dac_type,
                        dac.valid_flag AS dac_valid_flag,
                        dac.asset_class_rk AS dac_asset_class_rk,
                        dac4.BASEL_EXPOSURE_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE,
                        dce.P3_COUNTRY_DESC AS INCORP_P3_COUNTRY_DESC,
                        dcc2.COST_CENTRE,
                        dce2.P3_COUNTRY_DESC AS OP_P3_COUNTRY_DESC,
                        'EXPOSURE' FLOW_TYPE,
                        COALESCE (fctexpDNB.IFRS9_PROV_WRITE_OFF_AMT_GBP, 0)
                        IMP_WRITE_OFF_AMT,
                        CASE
                        WHEN fctexpDNB.B3_CVA_CC_RWA_AMT IS NOT NULL
                        THEN
                            'Y'
                        ELSE
                            'N'
                        END
                        CRD4_REP_CVA_FLAG,
                        IFRS9_BSPL_800_MAP.IAS39_CLASS,
                        CASE
                        WHEN (   dim_count.CIS_CODE = 'Z6VR0IT'
                                OR fctexpDNB.DEAL_ID LIKE 'AR2956%')
                        THEN
                            'OTHER'
                        ELSE
                            'IFRS9'
                        END
                        REP_PROVISION_TYPE,
                        CASE
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexpDNB.DEAL_ID LIKE
                                    '%_SI%'
                        THEN
                            'SI'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexpDNB.DEAL_ID LIKE
                                    '%_DTA%'
                        THEN
                            'DT'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexpDNB.DEAL_ID LIKE
                                    '%_NSI%'
                        THEN
                            'NSI'
                        ELSE
                            NULL
                        END
                        DEAL_TYPE,
                        CASE
                        WHEN fctexpDNB.APPROVED_APPROACH_CODE = 'STD'
                        THEN
                            fctexpDNB.B3_SME_DISCOUNT_FLAG
                        ELSE
                            NULL
                        END
                        STD_SME_DISCOUNT_APP_FLAG,
                        DIM_MATURITY.MATURITY_BAND_DESC,
                        CASE
                        WHEN fctexpDNB.REPORTING_TYPE_CODE = 'SR' THEN 'Y'
                        ELSE 'N'
                        END
                        CRD4_REP_SETTL_FLAG,
                        dim_count.FI_AVC_CATEGORY,
                        d_prod_cat2.COREP_PRODUCT_CATEGORY
                        COREP_EXPOSURE_CATEGORY,
                        pd_band.mgs AS dim_mgs,
                        pd_band.pd_band_code AS dim_pd_band_code,
                        CASE
                        WHEN dim_indust.UK_SIC_CODE = 'UNKNOWN' THEN NULL
                        ELSE dim_indust.UK_SIC_CODE
                        END
                        EC92_CODE,
                        dim_ind.sector_cluster,
                        dim_ind.SUB_SECTOR,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS MEMO_B2_STD_BASEL_EXPOSURE_TYPE,
                        GL_MASTER.BSPL_900_GL_CODE AS BSPL_900_GL_CODE,
                        GL_MASTER.BSPL_60_GL_CODE AS BSPL_60_GL_CODE,
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                                AS rprov_eff_from_year_month
                        FROM {rtd_ref_db}.etl_parameter
                        WHERE parameter_name = 'DE_BBK_IFRS9_PROV_EFF_DATE')
                        AS rprov_eff_from_year_month,
                        dprod.aiml_product_type AS aiml_product_type,
                        dce.P3_COUNTRY_DESC AS INCORP_COUNTRY_DESC,
                        dce2.P3_COUNTRY_DESC AS OP_COUNTRY_DESC,
                        COALESCE (dce.p3_country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_p3_country_desc,
                        COALESCE (dce.country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_country_desc,
                        DIM_RRU.LL_RRU_DESC AS RISK_TAKER_LL_RRU_DESC,
                        DIM_RRU2.LL_RRU_DESC AS RISK_ON_LL_RRU_DESC,
                        dim_counter.cis_code AS LE,
                        dim_count.SYS_CODE AS CSYS_CODE,
                        dim_count.SYS_ID AS CSYS_ID,
                        DIM_IND.SECTOR_TYPE,
                        DIM_IND.SECTOR,
                        dprod.HIGH_PROD,
                        dim_parent.cis_code AS parent_cis_code,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS B2_STD_BASEL_EXPOSURE_TYPE,
                        dim_rule.RULE_DESC,
                        dac4.BASEL_EXPOSURE_SUB_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE,
                        dac4.reg_exposure_type
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE,
                        dac4.TYPE AS B2_STD_PRE_DFLT_ASSET_CLASS_TYPE,
                        dcc2.cost_centre_type,
                        dac5.basel_exposure_sub_type
                        AS MEMO_B2_STD_BASEL_EXPOSURE_SUB_TYPE,
                        dac5.TYPE AS MEMO_B2_STD_TYPE,
                        dac5.reg_exposure_type
                        AS memo_b2_std_reg_exposure_type,
                        --fctexpDNB.snap_dt ,
                        --fctexpDNB.dataload_time AS original_time,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name = 'ICB_EFFECTIVE_DATE') AS INTEGER)
                        AS v_icb_eff_from_year_month_rk,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name =
                                    'CORE_TO_CORE_IG_EXP_EFF_DATE') AS INTEGER)
                        AS v_core_ig_eff_year_month_rk,
                        fctexpDNB.YEAR_MONTH_RK AS YEAR_MONTH,
                        COALESCE (
                        COALESCE (
                            (CASE
                                WHEN dim_wbu_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_wbu_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_count.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_count.counterparty_rk
                            END)),
                        -1)
                        AS LER_CHILD_COUNTERPARTY,
                        fctexpDNB.day_rk,
                        CASE
                        WHEN     fctexpDNB.REPORTING_REGULATOR_CODE =
                                    'CBI'
                                AND fctexpDNB.B2_APP_ADJUSTED_PD_RATIO = 1
                                AND dac.REG_EXPOSURE_SUB_TYPE =
                                    'Income Producing Real Estate'
                                AND dac.BASEL_EXPOSURE_TYPE = 'Corporate'
                                AND fctexpDNB.DEFINITIVE_SLOT_CATEGORY_CALC =
                                    'N'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN dprod.regulatory_product_type = 'PR_EQ_CIU'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN DIM_MODEL.model_code = 'SRW'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN DIM_MODEL.model_code = 'SG'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Object Finance',
                                                                'Commodities Finance')
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Income Producing Real Estate',
                                                                'Project Finance')
                        THEN
                            'AIRB - SL Slotting Approach'
                        ELSE
                            'AIRB - PD/LGD Approach'
                        END
                        RWA_CALC_METHOD,
                        fctexpDNB.B2_IRB_ASSET_CLASS_RK,
                        fctexpDNB.rru_rk AS risk_taker_rru,
                        fctexpDNB.risk_on_rru_rk AS risk_on_rru,
                        CASE
                        WHEN COALESCE (fctexpDNB.B2_APP_EAD_POST_CRM_AMT,
                                        0) = 0
                        THEN
                            'NULL EAD'
                        ELSE
                            CASE
                                WHEN fctexpDNB.COUNTERPARTY_RK = -1
                                THEN
                                    'RETAIL'
                                ELSE
                                    'CORP'
                            END
                        END
                        OBLIGOR_TYPE,
                        div.SUB1,
                        div.SUB1_DESC,
                        div.SUB2,
                        div.SUB2_DESC,
                        div.SUB3,
                        div.SUB3_DESC,
                        div.SUB4,
                        div.SUB4_DESC,
                        div.SUB5,
                        div.SUB5_DESC,
                        div.DIVISION_DESC
                FROM {scdl_db}.FCT_EXPOSURES_DNB fctexpDNB
                        JOIN VAR
                        ON VAR.rYear_Month_RK = fctexpDNB.YEAR_MONTH_RK
                        LEFT OUTER JOIN {scdl_db}.FCT_EXPOSURES_REC fctexpREC
                        ON (    fctexpDNB.DEAL_ID = fctexpREC.DEAL_ID
                            AND fctexpDNB.DAY_RK = fctexpREC.DAY_RK
                            AND fctexpDNB.YEAR_MONTH_RK =
                                    fctexpREC.YEAR_MONTH_RK
                            AND fctexpDNB.VALID_FLAG =
                                    fctexpREC.VALID_FLAG)
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU dim_rru
                        ON (    fctexpDNB.rru_rk = dim_rru.rru_rk
                            AND dim_rru.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_rru.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat
                        ON ( (CASE
                                    WHEN UPPER (fctexpDNB.ACLM_PRODUCT_TYPE) =
                                            'N/A'
                                    THEN
                                    'UNKNOWN'
                                    ELSE
                                    UPPER (fctexpDNB.ACLM_PRODUCT_TYPE)
                                END) = d_prod_cat.ACLM_PRODUCT_TYPE)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL DIM_MODEL
                        ON (    fctexpDNB.PD_MODEL_RK = DIM_MODEL.MODEL_RK
                            AND DIM_MODEL.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_MODEL.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac
                        ON (    fctexpDNB.B2_IRB_ASSET_CLASS_RK =
                                    dac.ASSET_CLASS_RK
                            AND dac.valid_from_date <= var.v_reporting_date
                            AND dac.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac2
                        ON (    fctexpDNB.B2_STD_ASSET_CLASS_RK =
                                    dac2.ASSET_CLASS_RK
                            AND dac2.TYPE = 'B2_STD'
                            AND dac2.BASEL_EXPOSURE_TYPE = 'Other items'
                            AND dac.valid_from_date <= var.v_reporting_date
                            AND dac.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES fct_count
                        ON (    fctexpDNB.COUNTERPARTY_RK =
                                    fct_count.COUNTERPARTY_RK
                            AND fctexpDNB.DAY_RK = fct_count.DAY_RK
                            AND fctexpDNB.YEAR_MONTH_RK =
                                    fct_count.YEAR_MONTH_RK
                            AND fct_count.VALID_FLAG = 'Y')
                        LEFT OUTER JOIN {scdl_db}.DIM_DIVISION div
                        ON (    fctexpDNB.DIVISION_RK = div.DIVISION_RK
                            AND div.valid_from_date <= var.v_reporting_date
                            AND div.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_PRODUCT dprod
                        ON (    fctexpDNB.PRODUCT_RK = dprod.PRODUCT_RK
                            AND dprod.valid_from_date <=
                                    var.v_reporting_date
                            AND dprod.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY dim_indust
                        ON (    fct_count.EC92_INDUSTRY_RK =
                                    dim_indust.INDUSTRY_RK
                            AND dim_indust.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_indust.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.dim_cost_centre dc
                        ON (    fctexpDNB.COST_CENTRE_RK =
                                    dc.COST_CENTRE_RK
                            AND dc.valid_from_date <= var.v_reporting_date
                            AND dc.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN counterparty_data dim_count
                        ON (    fctexpDNB.COUNTERPARTY_RK =
                                    dim_count.COUNTERPARTY_RK
                            AND dim_count.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_count.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN dcc dcc1
                        ON (dim_count.counterparty_rk =
                                dcc1.wbu_counterparty_rk)
                        LEFT JOIN counterparty_data dim_wbu_parent
                        ON (    dim_wbu_parent.counterparty_rk =
                                    dcc1.le_counterparty_rk
                            AND dim_wbu_parent.group_internal_flag = 'Y'
                            AND dim_wbu_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_wbu_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT JOIN counterparty_data dim_parent
                        ON (    dim_parent.counterparty_rk =
                                    fct_count.branch_parent_counterparty_rk
                            AND dim_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL MOD2
                        ON     fctexpDNB.EAD_MODEL_RK = MOD2.MODEL_RK
                            AND MOD2.valid_from_date <= var.v_reporting_date
                            AND MOD2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU DIM_RRU2
                        ON     fctexpDNB.RISK_ON_RRU_RK = dim_rru2.rru_rk
                            AND DIM_RRU2.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_RRU2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac3
                        ON     fctexpDNB.COREP_ASSET_CLASS_RK =
                                    dac3.ASSET_CLASS_RK
                            AND dac3.valid_from_date <= var.v_reporting_date
                            AND dac3.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN
                        {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat2
                        ON fctexpDNB.COREP_PRODUCT_CATEGORY_RK =
                                d_prod_cat2.COREP_PRODUCT_CATEGORY_RK
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac4
                        ON     fctexpDNB.B2_STD_PRE_DFLT_ASSET_CLASS_RK =
                                    dac4.ASSET_CLASS_RK
                            AND dac4.valid_from_date <= var.v_reporting_date
                            AND dac4.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce
                        ON     fctexpDNB.INCORP_COUNTRY_RK =
                                    dce.COUNTRY_RK
                            AND dce.valid_from_date <= var.v_reporting_date
                            AND dce.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_COST_CENTRE dcc2
                        ON     fctexpDNB.COST_CENTRE_RK =
                                    dcc2.COST_CENTRE_RK
                            AND dcc2.valid_flag = 'Y'
                            AND dcc2.valid_from_date <= var.v_reporting_date
                            AND dcc2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce2
                        ON     fctexpDNB.OP_COUNTRY_RK = dce2.COUNTRY_RK
                            AND dce2.valid_from_date <= var.v_reporting_date
                            AND dce2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.GL_MASTER GL_MASTER
                        ON (    fctexpDNB.GL_ACCOUNT_RK = GL_MASTER.GL_RK
                            AND GL_MASTER.valid_from_date <=
                                    var.v_reporting_date
                            AND GL_MASTER.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.IFRS9_BSPL_800_MAP IFRS9_BSPL_800_MAP
                        ON (GL_MASTER.BSPL_800_GL_CODE =
                                IFRS9_BSPL_800_MAP.BSPL800_CODE)
                        LEFT OUTER JOIN {scdl_db}.DIM_MATURITY_BAND DIM_MATURITY
                        ON (    (   (    (fctexpDNB.B2_IRB_EFFECTIVE_MATURITY_YRS) >
                                            DIM_MATURITY.LOWER_VALUE
                                        AND (fctexpDNB.B2_IRB_EFFECTIVE_MATURITY_YRS) <
                                            DIM_MATURITY.UPPER_VALUE)
                                    OR (    fctexpDNB.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.LOWER_VALUE
                                        AND DIM_MATURITY.LOWER_VAL_INCLUSIVE =
                                            'Y')
                                    OR (    fctexpDNB.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.UPPER_VALUE
                                        AND DIM_MATURITY.UPPER_VAL_INCLUSIVE =
                                            'Y'))
                            AND DIM_MATURITY.TYPE = 'S'
                            AND DIM_MATURITY.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_MATURITY.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_PD_BAND pd_band
                        ON (    (   (    COALESCE (
                                            fctexpDNB.b2_app_adjusted_pd_ratio,
                                            0.02501) > pd_band.lower_value
                                        AND COALESCE (
                                            fctexpDNB.b2_app_adjusted_pd_ratio,
                                            0.02501) < pd_band.upper_value)
                                    OR (    COALESCE (
                                            fctexpDNB.b2_app_adjusted_pd_ratio,
                                            0.02501) = pd_band.lower_value
                                        AND pd_band.lower_val_inclusive = 'Y')
                                    OR (    COALESCE (
                                            fctexpDNB.b2_app_adjusted_pd_ratio,
                                            0.02501) = pd_band.upper_value
                                        AND pd_band.upper_val_inclusive = 'Y'))
                            AND pd_band.TYPE = 'MGS'
                            AND pd_band.valid_from_date <=
                                    var.v_reporting_date
                            AND pd_band.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY DIM_IND
                        ON (    fctexpDNB.INDUSTRY_RK =
                                    DIM_IND.INDUSTRY_RK
                            AND DIM_IND.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_IND.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac5
                        ON     fctexpDNB.B2_STD_ASSET_CLASS_RK =
                                    dac5.ASSET_CLASS_RK
                            AND dac5.valid_from_date <= var.v_reporting_date
                            AND dac5.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN counterparty_data dim_counter
                        ON     dcc2.LE_COUNTERPARTY_RK =
                                    dim_counter.counterparty_rk
                            AND dim_counter.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_counter.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_op_country
                        ON     fctexpDNB.OP_COUNTRY_RK =
                                    d_op_country.COUNTRY_RK
                            AND d_op_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_op_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_incorp_country
                        ON     fctexpDNB.INCORP_COUNTRY_RK =
                                    d_incorp_country.COUNTRY_RK
                            AND d_incorp_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_incorp_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RULE dim_rule
                        ON fctexpDNB.STD_RW_PRIORITY_ORDER_RULE_RK =
                                dim_rule.RULE_RK
                WHERE     fctexpDNB.DAY_RK = VAR.rDay_rk
                        AND fctexpDNB.YEAR_MONTH_RK = VAR.rYear_Month_RK
                        AND (    (fctexpDNB.VALID_FLAG = 'Y')
                            AND (   (fctexpDNB.ADJUST_FLAG = 'U')
                                OR (fctexpDNB.ADJUST_FLAG = 'A')))
                        AND (CASE
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND fctexpDNB.ACLM_PRODUCT_TYPE IN ('BASEL-OTC',
                                                                        'REPO')
                                THEN
                                CASE
                                    WHEN     fctexpDNB.SYS_CODE IN ('SABFI2OR',
                                                                    'MUREX2OR',
                                                                    'MTH_OTH',
                                                                    'DAY_OTH')
                                        AND fctexpDNB.CAPITAL_CUTS = '4'
                                        AND JURISDICTION_FLAG = 'EU'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexpDNB.SYS_CODE,
                                                        'XX') NOT IN ('SABFI2OR',
                                                                    'MUREX2OR',
                                                                    'MTH_OTH',
                                                                    'DAY_OTH')
                                        AND (   fctexpDNB.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexpDNB.CAPITAL_CUTS =
                                                    ''
                                                OR LENGTH (
                                                    fctexpDNB.CAPITAL_CUTS) =
                                                    0)
                                        AND (   fctexpDNB.JURISDICTION_FLAG
                                                    IS NULL
                                                OR fctexpDNB.JURISDICTION_FLAG =
                                                    ''
                                                OR LENGTH (
                                                    fctexpDNB.JURISDICTION_FLAG) =
                                                    0)
                                    THEN
                                        'TRUE'
                                END
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND COALESCE (
                                            fctexpDNB.ACLM_PRODUCT_TYPE,
                                            'XX') NOT IN ('BASEL-OTC', 'REPO')
                                THEN
                                'TRUE'
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rdualrep_eff_from_yrmnth_rk
                                    AND VAR.rYear_Month_RK <
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                THEN
                                CASE
                                    WHEN     fctexpDNB.SYS_CODE IN ('SABFI2OR',
                                                                    'MUREX2OR')
                                        AND fctexpDNB.CRR2_FLAG = 'Y'
                                        AND fctexpDNB.CAPITAL_CUTS = '4'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexpDNB.SYS_CODE,
                                                        'XX') NOT IN ('SABFIMOR',
                                                                    'MUREXOR',
                                                                    'SABFI2OR',
                                                                    'MUREX2OR')
                                        AND COALESCE (fctexpDNB.CRR2_FLAG,
                                                        'N') <> 'Y'
                                        AND (   fctexpDNB.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexpDNB.CAPITAL_CUTS =
                                                    ''
                                                OR LENGTH (
                                                    fctexpDNB.CAPITAL_CUTS) =
                                                    0)
                                    THEN
                                        'TRUE'
                                END
                                WHEN VAR.rYear_Month_RK <
                                        VAR.rdualrep_eff_from_yrmnth_rk
                                THEN
                                'TRUE'
                            END = 'TRUE')
                UNION ALL
                SELECT NULL DEAL_ID_PREFIX,
                        fctexpOTH.DEAL_ID,
                        fctexpOTH.VALID_FLAG,
                        fctexpOTH.REPORTING_REGULATOR_CODE,
                        fctexpOTH.UNSETTLED_FACTOR_CODE,
                        fctexpOTH.UNSETTLED_PERIOD_CODE,
                        'A' ADJUST_FLAG,
                        fctexpOTH.REPORTING_TYPE_CODE,
                        fctexpOTH.BANK_BASE_ROLE_CODE,
                        fctexpOTH.ACCRUED_INT_ON_BAL_AMT,
                        fctexpOTH.ACCRUED_INTEREST_GBP,
                        fctexpOTH.APPROVED_APPROACH_CODE,
                        DIM_MODEL.MODEL_CODE AS PD_MODEL_CODE,
                        dac.REG_EXPOSURE_SUB_TYPE
                        AS b2_irb_asset_class_reg_exposure_sub_type,
                        dac.BASEL_EXPOSURE_TYPE
                        AS b2_irb_asset_class_basel_exposure_type,
                        dac2.BASEL_EXPOSURE_TYPE
                        AS b2_std_asset_class_basel_exposure_type,
                        fctexpOTH.B1_RWA_POST_CRM_AMT,
                        fctexpOTH.B2_APP_ADJUSTED_LGD_RATIO,
                        fctexpOTH.B2_APP_ADJUSTED_PD_RATIO,
                        fctexpOTH.B2_APP_EAD_PRE_CRM_AMT,
                        fctexpOTH.B2_IRB_EAD_PRE_CRM_AMT,
                        fctexpOTH.B2_STD_EAD_PRE_CRM_AMT,
                        fctexpOTH.B2_IRB_EAD_POST_CRM_AMT,
                        fctexpOTH.B2_STD_EAD_POST_CRM_AMT,
                        fctexpOTH.B2_IRB_RWA_POST_CRM_AMT,
                        fctexpOTH.B2_APP_EAD_POST_CRM_AMT,
                        fctexpOTH.B2_STD_RWA_POST_CRM_AMT,
                        fctexpOTH.B2_APP_EXPECTED_LOSS_AMT,
                        fctexpOTH.B2_IRB_NETTED_COLL_GBP,
                        fctexpOTH.B2_APP_RISK_WEIGHT_RATIO,
                        fctexpOTH.B2_APP_RWA_POST_CRM_AMT,
                        fctexpOTH.B2_IRB_DEFAULT_DATE,
                        fctexpOTH.B2_IRB_DRAWN_EAD,
                        fctexpOTH.B2_IRB_DRAWN_EXPECTED_LOSS,
                        fctexpOTH.B2_IRB_DRAWN_RWA,
                        fctexpOTH.B2_IRB_EFFECTIVE_MATURITY_YRS,
                        fctexpOTH.B2_IRB_ORIG_EXP_PRE_CON_FACTOR,
                        fctexpOTH.B2_IRB_UNDRAWN_EAD,
                        fctexpOTH.B2_IRB_UNDRAWN_EXPECTED_LOSS,
                        fctexpOTH.B2_IRB_UNDRAWN_RWA,
                        COALESCE (fctexpOTH.B2_STD_CCF, 0) / 100 B2_STD_CCF,
                        fctexpREC.B2_STD_CCF_LESS_THAN_YEAR,
                        fctexpREC.B2_STD_CCF_MORE_THAN_YEAR,
                        fctexpOTH.B2_STD_CREDIT_QUALITY_STEP,
                        fctexpOTH.B2_STD_DEFAULT_DATE,
                        fctexpOTH.B2_STD_DRAWN_EAD,
                        fctexpOTH.B2_STD_DRAWN_RWA,
                        fctexpOTH.B2_STD_EXP_HAIRCUT_ADJ_AMT,
                        fctexpOTH.SYS_CODE,
                        CASE
                        WHEN COALESCE (fctexpOTH.SYS_CODE, 'X') NOT IN ('ACBCNTNW',
                                                                        'ACBCNTRB',
                                                                        'LNIQNWTR',
                                                                        'LNIQRBTR')
                        THEN
                                COALESCE (
                                fctexpOTH.GROSS_DEP_UTILISATION_AMT,
                                0)
                            - COALESCE (fctexpOTH.NET_DEP_UTILISATION_AMT,
                                        0)
                        ELSE
                            0
                        END
                        B2_STD_NETTED_COLL_GBP,
                        fctexpOTH.GROSS_DEP_UTILISATION_AMT,
                        fctexpOTH.NET_DEP_UTILISATION_AMT,
                        fctexpOTH.B2_STD_ORIG_EXP_PRE_CON_FACTOR,
                        fctexpOTH.B2_STD_RISK_WEIGHT_RATIO,
                        fctexpOTH.B2_STD_UNDRAWN_EAD,
                        fctexpOTH.B2_STD_UNDRAWN_RWA,
                        fctexpOTH.B3_CCP_MARGIN_N_INITL_RWA_AMT,
                        fctexpOTH.B3_CCP_MARGIN_INITL_CASH_AMT,
                        fctexpOTH.B3_CCP_MAR_INITL_NON_CASH_AMT,
                        fctexpOTH.B3_CCP_MARGIN_INITL_RWA_AMT,
                        fctexpOTH.B3_CVA_PROVISION_AMT,
                        fctexpOTH.B3_CVA_PROVISION_RESIDUAL_AMT,
                        NULL JURISDICTION_FLAG,
                        fctexpOTH.CAPITAL_CUTS,
                        CASE
                        WHEN     var.rYear_Month_RK >=
                                    var.rpradiv_eff_from_yrmnth_rk
                                AND fctexpOTH.ACLM_PRODUCT_TYPE IN ('REPO',
                                                                    'BASEL-OTC')
                        THEN
                            CASE
                                WHEN     fctexpOTH.SYS_CODE IN ('SABFI2OR',
                                                                'MUREX2OR',
                                                                'MTH_OTH',
                                                                'DAY_OTH')
                                    AND '' = 'EU'
                                    AND fctexpOTH.CAPITAL_CUTS = '4'
                                THEN
                                    'Y'
                                ELSE
                                    fctexpOTH.BASEL_DATA_FLAG
                            END
                        WHEN     var.rYear_Month_RK <
                                    var.rpradiv_eff_from_yrmnth_rk
                                AND var.rYear_Month_RK >=
                                    var.rdualrep_eff_from_yrmnth_rk
                        THEN
                            CASE
                                WHEN     fctexpOTH.SYS_CODE IN ('SABFI2OR',
                                                                'MUREX2OR')
                                    AND fctexpOTH.CRR2_FLAG = 'Y'
                                    AND fctexpOTH.CAPITAL_CUTS = '4'
                                THEN
                                    'Y'
                                ELSE
                                    fctexpOTH.BASEL_DATA_FLAG
                            END
                        ELSE
                            fctexpOTH.BASEL_DATA_FLAG
                        END
                        BASEL_DATA_FLAG,
                        -- fctexpOTH.BASEL_DATA_FLAG,
                        fctexpOTH.CRR2_FLAG,
                        fctexpOTH.COLL_GOOD_PROV_AMT,
                        fctexpOTH.COMM_LESS_THAN_1_YEAR,
                        fctexpOTH.COMM_MORE_THAN_1_YEAR,
                        fctexpOTH.CVA_CHARGE,
                        fctexpOTH.IFRS9_PROV_IMPAIRMENT_AMT_GBP,
                        fctexpOTH.IFRS9_PROV_WRITE_OFF_AMT_GBP,
                        fctexpOTH.INTERNAL_TRANSACTION_FLAG,
                        fctexpOTH.NET_EXPOSURE,
                        fctexpOTH.NET_EXPOSURE_POST_SEC,
                        fctexpOTH.OFF_BAL_LED_EXP_GBP_POST_SEC,
                        fctexpOTH.OFF_BALANCE_EXPOSURE,
                        fctexpOTH.ON_BAL_LED_EXP_GBP_POST_SEC,
                        fctexpOTH.ON_BALANCE_LEDGER_EXPOSURE,
                        fctexpOTH.PAST_DUE_ASSET_FLAG,
                        fctexpOTH.PROVISION_AMOUNT_GBP,
                        fctexpOTH.RESIDUAL_MATURITY_DAYS,
                        fctexpOTH.RESIDUAL_VALUE,
                        fctexpOTH.RETAIL_OFF_BAL_SHEET_FLAG,
                        fctexpOTH.RISK_ON_GROUP_STRUCTURE,
                        fctexpOTH.RISK_TAKER_GROUP_STRUCTURE,
                        fctexpOTH.SECURITISED_FLAG,
                        fctexpOTH.SET_OFF_INDICATOR,
                        fctexpOTH.SRT_FLAG,
                        fctexpOTH.TRADING_BOOK_FLAG,
                        fctexpOTH.TYPE,
                        fctexpOTH.UNDRAWN_COMMITMENT_AMT,
                        fctexpOTH.UNDRAWN_COMMITMENT_AMT_PST_SEC,
                        fctexpOTH.UNSETTLED_AMOUNT,
                        fctexpOTH.UNSETTLED_PRICE_DIFFERENCE_AMT,
                        fctexpOTH.LEASE_EXPOSURE_GBP,
                        fctexpOTH.LEASE_PAYMENT_VALUE_AMT,
                        fctexpOTH.OBLIGORS_COUNT,
                        fctexpOTH.STD_EXPOSURE_DEFAULT_FLAG,
                        fctexpOTH.CRD4_SOV_SUBSTITUTION_FLAG,
                        fctexpOTH.UNLIKELY_PAY_FLAG,
                        fctexpOTH.CRD4_STD_RP_SME_Flag,
                        fctexpOTH.CRD4_IRB_RP_SME_Flag,
                        fctexpOTH.B3_SME_DISCOUNT_FLAG,
                        fctexpOTH.IRB_SME_DISCOUNT_APP_FLAG,
                        fctexpOTH.TRANSITIONAL_PORTFOLIO_FLAG,
                        fctexpOTH.FULLY_COMPLETE_SEC_FLAG,
                        fctexpOTH.SAF_FILM_LEASING_FLAG,
                        fctexpOTH.PRINCIPAL_AMT,
                        fctexpOTH.MTM,
                        fctexpOTH.AMORTISATION_AMT,
                        fct_count.BSD_MARKER,
                        dim_indust.UK_SIC_CODE,
                        fctexpOTH.IMMOVABLE_PROP_INDICATOR,
                        fctexpOTH.COREP_CRM_REPORTING_FLAG,
                        fctexpOTH.QUAL_REV_EXP_FLAG,
                        fctexpOTH.COMM_REAL_ESTATE_SEC_FLAG,
                        fctexpREC.MORTGAGES_FLAG,
                        fctexpOTH.PERS_ACC_FLAG,
                        fctexpOTH.CCP_RISK_WEIGHT,
                        fctexpOTH.CS_PROXY_USED_FLAG,
                        fctexpOTH.B2_IRB_INTEREST_AT_DEFAULT_GBP,
                        fctexpOTH.REIL_AMT_GBP,
                        fctexpOTH.REIL_STATUS,
                        fct_count.RP_TURNOVER_MAX_EUR,
                        fct_count.RP_TURNOVER_CCY,
                        fct_count.RP_SALES_TURNOVER_SOURCE,
                        fctexpOTH.B3_SME_RP_FLAG,
                        fctexpOTH.B3_SME_RP_IRB_CORP_FLAG,
                        fct_count.B3_SME_RP_1_5M_FLAG,
                        fct_count.RP_SALES_TURNOVER_LOCAL,
                        fct_count.TOTAL_ASSET_LOCAL,
                        fct_count.LE_TOTAL_ASSET_AMOUNT_EUR,
                        fctexpOTH.MARKET_RISK_SUB_TYPE,
                        fctexpOTH.LE_FI_AVC_CATEGORY,
                        fctexpOTH.B2_IRB_RWA_R_COR_COEFF,
                        fctexpOTH.B2_IRB_RWA_K_CAPITAL_REQ_PRE,
                        fctexpOTH.B2_IRB_RWA_K_CAPITAL_REQ_POST,
                        fctexpOTH.SCALE_UP_COEFENT_OF_CORELATION,
                        fctexpOTH.B3_CVA_CC_ADV_FLAG,
                        dim_rru.LLP_RRU,
                        dim_rru.RRU_RK,
                        fctexpOTH.B3_CVA_CC_ADV_VAR_AVG,
                        fctexpOTH.B3_CVA_CC_ADV_VAR_SPOT,
                        fctexpOTH.B3_CVA_CC_ADV_VAR_STRESS_AVG,
                        fctexpOTH.B3_CVA_CC_ADV_VAR_STRESS_SPOT,
                        fctexpOTH.B3_CVA_CC_CAPITAL_AMT,
                        fctexpOTH.B3_CVA_CC_RWA_AMT,
                        fctexpOTH.B3_CVA_CC_CDS_SNGL_NM_NTNL_AMT,
                        fctexpOTH.B3_CVA_CC_CDS_INDEX_NTNL_AMT,
                        fctexpOTH.B2_ALGO_IRB_CORP_SME_FLAG,
                        fctexpOTH.STATUTORY_LEDGER_BALANCE,
                        fctexpOTH.CARRYING_VALUE,
                        fctexpOTH.LEVERAGE_EXPOSURE,
                        fct_count.STANDALONE_ENTITY,
                        fct_count.RP_TURNOVER_SOURCE_TYPE,
                        fctexpOTH.IFRS9_FINAL_IIS_GBP,
                        fctexpOTH.TOT_REGU_PROV_AMT_GBP,
                        fctexpOTH.NET_MKT_VAL,
                        fctexpOTH.B2_APP_CAP_REQ_POST_CRM_AMT,
                        fctexpOTH.GRDW_POOL_ID,
                        fctexpOTH.NEW_IN_DEFAULT_FLAG,
                        fctexpOTH.RETAIL_POOL_SME_FLAG,
                        fctexpOTH.GRDW_POOL_GROUP_ID,
                        COALESCE (fctexpOTH.B2_IRB_CCF, 0) / 100 B2_IRB_CCF,
                        fctexpOTH.DRAWN_LEDGER_BALANCE,
                        fctexpOTH.E_STAR_GROSS_EXPOSURE,
                        fctexpOTH.B2_IRB_RWA_PRE_CDS,
                        fctexpOTH.LGD_CALC_MODE,
                        fctexpOTH.DEFINITIVE_SLOT_CATEGORY_CALC,
                        fctexpOTH.REPU_COMM_MORE_THAN_YEAR_GBP,
                        fctexpOTH.REPU_COMM_LESS_THAN_YEAR_GBP,
                        fctexpOTH.UNCOMM_UNDRAWN_AMT_GBP,
                        fctexpOTH.TOTAL_UNDRAWN_AMOUNT_GBP,
                        fctexpOTH.COLLECTIVELY_EVALUATED_FLAG,
                        fctexpOTH.ORIGINAL_CURRENCY_CODE,
                        fctexpOTH.ORIGINAL_TRADE_ID,
                        fctexpOTH.E_STAR_NET_EXPOSURE,
                        fctexpOTH.RISK_TAKER_RF_FLAG,
                        fctexpOTH.RISK_TAKER_GS_CODE,
                        fctexpOTH.RISK_ON_RF_FLAG,
                        fctexpOTH.RISK_ON_GS_CODE,
                        fctexpOTH.BORROWER_GS_CODE,
                        fctexpOTH.STS_SECURITISATION,
                        fctexpOTH.STS_SEC_QUAL_CAP_TRTMNT,
                        fctexpOTH.STS_SEC_APPROACH_CODE,
                        fctexpOTH.B3_APP_RWA_INC_CVA_CC_AMT,
                        fctexpOTH.IFRS9_TOT_REGU_ECL_AMT_GBP,
                        fctexpOTH.IFRS9_FINAL_ECL_MES_GBP,
                        fctexpOTH.IFRS9_FINAL_STAGE,
                        fctexpOTH.IFRS9_STAGE_3_TYPE,
                        fctexpOTH.IFRS9_ECL_CURRENCY_CODE,
                        fctexpOTH.IFRS9_TOT_STATUT_ECL_AMT_GBP,
                        fctexpOTH.IFRS9_DISCNT_UNWIND_AMT_GBP,
                        fctexpOTH.B1_EAD_POST_CRM_AMT,
                        fctexpOTH.B1_RISK_WEIGHT_RATIO,
                        fctexpOTH.B1_RWA_PRE_CRM_AMT,
                        fctexpOTH.B1_UNDRAWN_COMMITMENT_GBP,
                        fctexpOTH.B2_IRB_RWA_PRE_CRM_AMT,
                        fctexpOTH.B2_STD_RWA_PRE_CRM_AMT,
                        fctexpOTH.FACILITY_MATCHING_LEVEL,
                        fctexpREC.PRODUCT_FAMILY_CODE,
                        fctexpOTH.CARTHESIS_REPORTING_ID,
                        fctexpOTH.NGAAP_PROVISION_AMT_GBP,
                        dim_count.CIS_CODE AS EXTERNAL_COUNTERPARTY_ID_TEMP,
                        dim_count.RISK_ENTITY_CLASS
                        AS EXTERNAL_COUNTERPARTY_RISK_CLASS,
                        dim_count.QCCP AS External_COUNTERPARTY_QCCP_FLAG,
                        fctexpOTH.SME_DISCOUNT_FACTOR,
                        fctexpOTH.CRR2_501A_DISCOUNT_FLAG,
                        dac.REG_EXPOSURE_TYPE
                        AS b2_irb_asset_class_reg_exposure_type,
                        dac.basel_exposure_sub_type
                        AS b2_irb_asset_class_basel_exposure_sub_type,
                        dac2.basel_exposure_sub_type
                        AS b2_std_asset_class_basel_exposure_sub_type,
                        div.SUB2 AS DIVISION_SUB2,
                        div.DIVISION AS DIVISION,
                        dprod.REGULATORY_PRODUCT_TYPE
                        AS REGULATORY_PRODUCT_TYPE,
                        dim_wbu_parent.CIS_CODE AS ILE_COUNTERPARTY_CISCODE,
                        dim_wbu_parent.RISK_ENTITY_CLASS AS ILE_RISK_CLASS,
                        dim_parent.CIS_CODE AS BRANCH_CISCODE,
                        dim_parent.RISK_ENTITY_CLASS AS BRANCH_RISK_CLASS,
                        --'N' CRD4_REP_STDFLOW_FLAG,
                        CASE
                        WHEN div.SUB2 IN ('CITIZENS2001',
                                            'CITIZENS2002',
                                            'CITIZENS2003',
                                            'CITIZENS2001_NC',
                                            'CITIZENS2002_NC',
                                            'CITIZENS2003_NC')
                        THEN
                            'Y'
                        ELSE
                            'N'
                        END
                        CITIZENS_OVERRIDE_FLAG,
                        'N' CRD4_REP_STDFLOW_FLAG,
                        CASE
                        WHEN (CASE
                                    WHEN div.SUB2 IN ('CITIZENS2001',
                                                    'CITIZENS2002',
                                                    'CITIZENS2003',
                                                    'CITIZENS2001_NC',
                                                    'CITIZENS2002_NC',
                                                    'CITIZENS2003_NC')
                                    THEN
                                    'Y'
                                    ELSE
                                    'N'
                                END) = 'Y'
                        THEN
                            COALESCE (fctexpOTH.B2_STD_EAD_POST_CRM_AMT, 0)
                        ELSE
                            COALESCE (fctexpOTH.B2_APP_EAD_POST_CRM_AMT, 0)
                        END
                        EAD_POST_CRM_AMT,
                        CASE
                        WHEN     REPORTING_TYPE_CODE = 'NC'
                                AND dac2.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            'Other items'
                        ELSE
                            dac2.BASEL_EXPOSURE_TYPE
                        END
                        COREP_STD_BASEL_EXPOSURE_TYPE,
                        CASE
                        WHEN d_prod_cat.COREP_PRODUCT_CATEGORY_RK = -1
                        THEN
                            'UNKNOWN'
                        ELSE
                            d_prod_cat.COREP_PRODUCT_CATEGORY
                        END
                        AS COREP_PRODUCT_CATEGORY,
                        dim_indust.SECTOR_TYPE AS INDUSTRY_SECTOR_TYPE,
                        DIM_RRU.LL_RRU AS RISK_TAKER_LL_RRU,
                        dim_count.SOVEREIGN_REGIONAL_GOVT_FLAG
                        AS SOVEREIGN_REGIONAL_GOVT_FLAG,
                        MOD2.MODEL_CODE AS EAD_MODEL_CODE,
                        dim_rru2.ll_rru AS RISK_ON_LL_RRU,
                        CASE
                        WHEN     dac3.basel_exposure_type = 'Retail'
                                AND dac3.basel_exposure_sub_type = 'N/A'
                        THEN
                            CASE
                                WHEN fctexpOTH.QUAL_REV_EXP_FLAG = 'Y'
                                THEN
                                    'Qualifying Revolving Exposures'
                                ELSE
                                    CASE
                                    WHEN    fctexpOTH.COMM_REAL_ESTATE_SEC_FLAG =
                                                'Y'
                                            OR fctexpREC.MORTGAGES_FLAG = 'Y'
                                    THEN
                                        CASE
                                            WHEN fctexpOTH.PERS_ACC_FLAG =
                                                    'N'
                                            THEN
                                                'Secured by Real Estate Property - SME'
                                            ELSE
                                                'Secured by Real Estate Property - Non-SME'
                                        END
                                    ELSE
                                        CASE
                                            WHEN fctexpOTH.PERS_ACC_FLAG =
                                                    'N'
                                            THEN
                                                'Other Retail Exposures - SME'
                                            ELSE
                                                'Other Retail Exposures - Non-SME'
                                        END
                                    END
                            END
                        ELSE
                            dac3.basel_exposure_sub_type
                        END
                        COREP_IRB_EXPOSURE_SUB_TYPE,
                        UPPER (fctexpOTH.ACLM_PRODUCT_TYPE)
                        AS ACLM_PRODUCT_TYPE,
                        d_prod_cat2.ACLM_PRODUCT_TYPE
                        AS ACLM_PRODUCT_TYPE_d_prod_cat2,
                        (SELECT PaRAM_VALUE
                        FROM {scdl_db}.dim_rwa_engine_generic_params
                        WHERE param_code = 'INFRADISCOUNTFACTOR')
                        AS v_infra_discount_factor,
                        CASE
                        WHEN     fctexpOTH.REPORTING_TYPE_CODE = 'NC'
                                AND dac5.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            var.v_oth_itm_asset_class_rk
                        ELSE
                            fctexpOTH.B2_STD_ASSET_CLASS_RK
                        END
                        COREP_STD_ASSET_CLASS_RK,
                        dac.TYPE AS dac_type,
                        dac.valid_flag AS dac_valid_flag,
                        dac.asset_class_rk AS dac_asset_class_rk,
                        dac4.BASEL_EXPOSURE_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE,
                        dce.P3_COUNTRY_DESC AS INCORP_P3_COUNTRY_DESC,
                        dcc2.COST_CENTRE,
                        dce2.P3_COUNTRY_DESC AS OP_P3_COUNTRY_DESC,
                        'EXPOSURE' FLOW_TYPE,
                        COALESCE (fctexpOTH.IFRS9_PROV_WRITE_OFF_AMT_GBP, 0)
                        IMP_WRITE_OFF_AMT,
                        CASE
                        WHEN fctexpOTH.B3_CVA_CC_RWA_AMT IS NOT NULL
                        THEN
                            'Y'
                        ELSE
                            'N'
                        END
                        CRD4_REP_CVA_FLAG,
                        IFRS9_BSPL_800_MAP.IAS39_CLASS,
                        CASE
                        WHEN (   dim_count.CIS_CODE = 'Z6VR0IT'
                                OR fctexpOTH.DEAL_ID LIKE 'AR2956%')
                        THEN
                            'OTHER'
                        ELSE
                            'IFRS9'
                        END
                        REP_PROVISION_TYPE,
                        CASE
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexpOTH.DEAL_ID LIKE
                                    '%_SI%' 
                        THEN
                            'SI'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexpOTH.DEAL_ID LIKE
                                    '%/_DTA%' 
                        THEN
                            'DT'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexpOTH.DEAL_ID LIKE
                                    '%/_NSI%' 
                        THEN
                            'NSI'
                        ELSE
                            NULL
                        END
                        DEAL_TYPE,
                        CASE
                        WHEN fctexpOTH.APPROVED_APPROACH_CODE = 'STD'
                        THEN
                            fctexpOTH.B3_SME_DISCOUNT_FLAG
                        ELSE
                            NULL
                        END
                        STD_SME_DISCOUNT_APP_FLAG,
                        DIM_MATURITY.MATURITY_BAND_DESC,
                        CASE
                        WHEN fctexpOTH.REPORTING_TYPE_CODE = 'SR' THEN 'Y'
                        ELSE 'N'
                        END
                        CRD4_REP_SETTL_FLAG,
                        dim_count.FI_AVC_CATEGORY,
                        d_prod_cat2.COREP_PRODUCT_CATEGORY
                        COREP_EXPOSURE_CATEGORY,
                        pd_band.mgs AS dim_mgs,
                        pd_band.pd_band_code AS dim_pd_band_code,
                        CASE
                        WHEN dim_indust.UK_SIC_CODE = 'UNKNOWN' THEN NULL
                        ELSE dim_indust.UK_SIC_CODE
                        END
                        EC92_CODE,
                        dim_ind.sector_cluster,
                        dim_ind.SUB_SECTOR,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS MEMO_B2_STD_BASEL_EXPOSURE_TYPE,
                        GL_MASTER.BSPL_900_GL_CODE AS BSPL_900_GL_CODE,
                        GL_MASTER.BSPL_60_GL_CODE AS BSPL_60_GL_CODE,
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                                AS rprov_eff_from_year_month
                        FROM {rtd_ref_db}.etl_parameter
                        WHERE parameter_name = 'DE_BBK_IFRS9_PROV_EFF_DATE')
                        AS rprov_eff_from_year_month,
                        dprod.aiml_product_type AS aiml_product_type,
                        dce.P3_COUNTRY_DESC AS INCORP_COUNTRY_DESC,
                        dce2.P3_COUNTRY_DESC AS OP_COUNTRY_DESC,
                        COALESCE (dce.p3_country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_p3_country_desc,
                        COALESCE (dce.country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_country_desc,
                        DIM_RRU.LL_RRU_DESC AS RISK_TAKER_LL_RRU_DESC,
                        DIM_RRU2.LL_RRU_DESC AS RISK_ON_LL_RRU_DESC,
                        dim_counter.cis_code AS LE,
                        dim_count.SYS_CODE AS CSYS_CODE,
                        dim_count.SYS_ID AS CSYS_ID,
                        DIM_IND.SECTOR_TYPE,
                        DIM_IND.SECTOR,
                        dprod.HIGH_PROD,
                        dim_parent.cis_code AS parent_cis_code,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS B2_STD_BASEL_EXPOSURE_TYPE,
                        dim_rule.RULE_DESC,
                        dac4.BASEL_EXPOSURE_SUB_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE,
                        dac4.reg_exposure_type
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE,
                        dac4.TYPE AS B2_STD_PRE_DFLT_ASSET_CLASS_TYPE,
                        dcc2.cost_centre_type,
                        dac5.basel_exposure_sub_type
                        AS MEMO_B2_STD_BASEL_EXPOSURE_SUB_TYPE,
                        dac5.TYPE AS MEMO_B2_STD_TYPE,
                        dac5.reg_exposure_type
                        AS memo_b2_std_reg_exposure_type,
                        --fctexpOTH.snap_dt ,
                        --fctexpOTH.dataload_time AS original_time,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name = 'ICB_EFFECTIVE_DATE') AS INTEGER)
                        AS v_icb_eff_from_year_month_rk,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name =
                                    'CORE_TO_CORE_IG_EXP_EFF_DATE') AS INTEGER)
                        AS v_core_ig_eff_year_month_rk,
                        fctexpOTH.year_month_rk AS YEAR_MONTH,
                        COALESCE (
                        COALESCE (
                            (CASE
                                WHEN dim_wbu_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_wbu_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_count.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_count.counterparty_rk
                            END)),
                        -1)
                        AS LER_CHILD_COUNTERPARTY,
                        fctexpOTH.day_rk,
                        CASE
                        WHEN     fctexpOTH.REPORTING_REGULATOR_CODE =
                                    'CBI'
                                AND fctexpOTH.B2_APP_ADJUSTED_PD_RATIO = 1
                                AND dac.REG_EXPOSURE_SUB_TYPE =
                                    'Income Producing Real Estate'
                                AND dac.BASEL_EXPOSURE_TYPE = 'Corporate'
                                AND fctexpOTH.DEFINITIVE_SLOT_CATEGORY_CALC =
                                    'N'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN dprod.regulatory_product_type = 'PR_EQ_CIU'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN DIM_MODEL.model_code = 'SRW'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN DIM_MODEL.model_code = 'SG'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Object Finance',
                                                                'Commodities Finance')
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Income Producing Real Estate',
                                                                'Project Finance')
                        THEN
                            'AIRB - SL Slotting Approach'
                        ELSE
                            'AIRB - PD/LGD Approach'
                        END
                        RWA_CALC_METHOD,
                        fctexpOTH.b2_irb_asset_class_rk,
                        fctexpOTH.rru_rk AS risk_taker_rru,
                        fctexpOTH.risk_on_rru_rk AS risk_on_rru,
                        CASE
                        WHEN COALESCE (fctexpOTH.B2_APP_EAD_POST_CRM_AMT,
                                        0) = 0
                        THEN
                            'NULL EAD'
                        ELSE
                            CASE
                                WHEN fctexpOTH.COUNTERPARTY_RK = -1
                                THEN
                                    'RETAIL'
                                ELSE
                                    'CORP'
                            END
                        END
                        OBLIGOR_TYPE,
                        div.SUB1,
                        div.SUB1_DESC,
                        div.SUB2,
                        div.SUB2_DESC,
                        div.SUB3,
                        div.SUB3_DESC,
                        div.SUB4,
                        div.SUB4_DESC,
                        div.SUB5,
                        div.SUB5_DESC,
                        div.DIVISION_DESC
                FROM {scdl_db}.FCT_EXPOSURES_OTH fctexpOTH
                        JOIN VAR
                        ON VAR.rYear_Month_RK = fctexpOTH.YEAR_MONTH_RK
                        LEFT OUTER JOIN {scdl_db}.FCT_EXPOSURES_REC fctexpREC
                        ON (    fctexpOTH.DEAL_ID = fctexpREC.DEAL_ID
                            AND fctexpOTH.DAY_RK = fctexpREC.DAY_RK
                            AND fctexpOTH.YEAR_MONTH_RK =
                                    fctexpREC.YEAR_MONTH_RK
                            AND fctexpOTH.VALID_FLAG =
                                    fctexpREC.VALID_FLAG)
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU DIM_RRU
                        ON (    fctexpOTH.rru_rk = dim_rru.rru_rk
                            AND DIM_RRU.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_RRU.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat
                        ON ( (CASE
                                    WHEN UPPER (fctexpOTH.ACLM_PRODUCT_TYPE) =
                                            'N/A'
                                    THEN
                                    'UNKNOWN'
                                    ELSE
                                    UPPER (fctexpOTH.ACLM_PRODUCT_TYPE)
                                END) = d_prod_cat.ACLM_PRODUCT_TYPE)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES fct_count
                        ON (    fctexpOTH.COUNTERPARTY_RK =
                                    fct_count.COUNTERPARTY_RK
                            AND fctexpOTH.DAY_RK = fct_count.DAY_RK
                            AND fctexpOTH.YEAR_MONTH_RK =
                                    fct_count.YEAR_MONTH_RK
                            AND fct_count.VALID_FLAG = 'Y')
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY dim_indust
                        ON (    fct_count.EC92_INDUSTRY_RK =
                                    dim_indust.INDUSTRY_RK
                            AND dim_indust.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_indust.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL DIM_MODEL
                        ON (    fctexpOTH.PD_MODEL_RK = DIM_MODEL.MODEL_RK
                            AND DIM_MODEL.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_MODEL.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac
                        ON (    fctexpOTH.B2_IRB_ASSET_CLASS_RK =
                                    dac.ASSET_CLASS_RK
                            AND dac.valid_from_date <= var.v_reporting_date
                            AND dac.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac2
                        ON (    fctexpOTH.B2_STD_ASSET_CLASS_RK =
                                    dac2.ASSET_CLASS_RK
                            AND dac2.TYPE = 'B2_STD'
                            AND dac2.BASEL_EXPOSURE_TYPE = 'Other items'
                            AND dac2.valid_from_date <= var.v_reporting_date
                            AND dac2.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_DIVISION div
                        ON (    fctexpOTH.DIVISION_RK = div.DIVISION_RK
                            AND div.valid_from_date <= var.v_reporting_date
                            AND div.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_PRODUCT dprod
                        ON (    fctexpOTH.PRODUCT_RK = dprod.PRODUCT_RK
                            AND dprod.valid_from_date <=
                                    var.v_reporting_date
                            AND dprod.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.dim_cost_centre dc
                        ON (    fctexpOTH.COST_CENTRE_RK =
                                    dc.COST_CENTRE_RK
                            AND dc.valid_from_date <= var.v_reporting_date
                            AND dc.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN counterparty_data dim_count
                        ON (    fctexpOTH.COUNTERPARTY_RK =
                                    dim_count.COUNTERPARTY_RK
                            AND dim_count.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_count.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN dcc dcc1
                        ON (dim_count.counterparty_rk =
                                dcc1.wbu_counterparty_rk)
                        LEFT JOIN counterparty_data dim_wbu_parent
                        ON (    dim_wbu_parent.counterparty_rk =
                                    dcc1.le_counterparty_rk
                            AND dim_wbu_parent.group_internal_flag = 'Y'
                            AND dim_wbu_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_wbu_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT JOIN counterparty_data dim_parent
                        ON (    dim_parent.counterparty_rk =
                                    fct_count.branch_parent_counterparty_rk
                            AND dim_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL MOD2
                        ON     fctexpOTH.EAD_MODEL_RK = MOD2.MODEL_RK
                            AND MOD2.valid_from_date <= var.v_reporting_date
                            AND MOD2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU DIM_RRU2
                        ON     fctexpOTH.RISK_ON_RRU_RK = dim_rru2.rru_rk
                            AND DIM_RRU2.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_RRU2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac3
                        ON     fctexpOTH.COREP_ASSET_CLASS_RK =
                                    dac3.ASSET_CLASS_RK
                            AND dac3.valid_from_date <= var.v_reporting_date
                            AND dac3.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN
                        {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat2
                        ON fctexpOTH.COREP_PRODUCT_CATEGORY_RK =
                                d_prod_cat2.COREP_PRODUCT_CATEGORY_RK
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac4
                        ON     fctexpOTH.B2_STD_PRE_DFLT_ASSET_CLASS_RK =
                                    dac4.ASSET_CLASS_RK
                            AND dac4.valid_from_date <= var.v_reporting_date
                            AND dac4.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce
                        ON     fctexpOTH.INCORP_COUNTRY_RK =
                                    dce.COUNTRY_RK
                            AND dce.valid_from_date <= var.v_reporting_date
                            AND dce.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_COST_CENTRE dcc2
                        ON (    fctexpOTH.COST_CENTRE_RK =
                                    dcc2.COST_CENTRE_RK
                            AND dcc2.valid_flag = 'Y'
                            AND dcc2.valid_from_date <= var.v_reporting_date
                            AND dcc2.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce2
                        ON     fctexpOTH.OP_COUNTRY_RK = dce2.COUNTRY_RK
                            AND dce2.valid_from_date <= var.v_reporting_date
                            AND dce2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.GL_MASTER GL_MASTER
                        ON (    fctexpOTH.GL_ACCOUNT_RK = GL_MASTER.GL_RK
                            AND GL_MASTER.valid_from_date <=
                                    var.v_reporting_date
                            AND GL_MASTER.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.IFRS9_BSPL_800_MAP IFRS9_BSPL_800_MAP
                        ON (GL_MASTER.BSPL_800_GL_CODE =
                                IFRS9_BSPL_800_MAP.BSPL800_CODE)
                        LEFT OUTER JOIN {scdl_db}.DIM_MATURITY_BAND DIM_MATURITY
                        ON (    (   (    (fctexpOTH.B2_IRB_EFFECTIVE_MATURITY_YRS) >
                                            DIM_MATURITY.LOWER_VALUE
                                        AND (fctexpOTH.B2_IRB_EFFECTIVE_MATURITY_YRS) <
                                            DIM_MATURITY.UPPER_VALUE)
                                    OR (    fctexpOTH.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.LOWER_VALUE
                                        AND DIM_MATURITY.LOWER_VAL_INCLUSIVE =
                                            'Y')
                                    OR (    fctexpOTH.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.UPPER_VALUE
                                        AND DIM_MATURITY.UPPER_VAL_INCLUSIVE =
                                            'Y'))
                            AND DIM_MATURITY.TYPE = 'S'
                            AND DIM_MATURITY.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_MATURITY.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_PD_BAND pd_band
                        ON (    (   (    COALESCE (
                                            fctexpOTH.b2_app_adjusted_pd_ratio,
                                            0.02501) > pd_band.lower_value
                                        AND COALESCE (
                                            fctexpOTH.b2_app_adjusted_pd_ratio,
                                            0.02501) < pd_band.upper_value)
                                    OR (    COALESCE (
                                            fctexpOTH.b2_app_adjusted_pd_ratio,
                                            0.02501) = pd_band.lower_value
                                        AND pd_band.lower_val_inclusive = 'Y')
                                    OR (    COALESCE (
                                            fctexpOTH.b2_app_adjusted_pd_ratio,
                                            0.02501) = pd_band.upper_value
                                        AND pd_band.upper_val_inclusive = 'Y'))
                            AND pd_band.TYPE = 'MGS'
                            AND pd_band.valid_from_date <=
                                    var.v_reporting_date
                            AND pd_band.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY DIM_IND
                        ON (    fctexpOTH.INDUSTRY_RK =
                                    DIM_IND.INDUSTRY_RK
                            AND DIM_IND.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_IND.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac5
                        ON     fctexpOTH.B2_STD_ASSET_CLASS_RK =
                                    dac5.ASSET_CLASS_RK
                            AND dac5.valid_from_date <= var.v_reporting_date
                            AND dac5.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN counterparty_data dim_counter
                        ON     dcc2.LE_COUNTERPARTY_RK =
                                    dim_counter.counterparty_rk
                            AND dim_counter.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_counter.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_op_country
                        ON     fctexpOTH.OP_COUNTRY_RK =
                                    d_op_country.COUNTRY_RK
                            AND d_op_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_op_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_incorp_country
                        ON     fctexpOTH.INCORP_COUNTRY_RK =
                                    d_incorp_country.COUNTRY_RK
                            AND d_incorp_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_incorp_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RULE dim_rule
                        ON fctexpOTH.STD_RW_PRIORITY_ORDER_RULE_RK =
                                dim_rule.RULE_RK
                WHERE     fctexpOTH.REPORTING_REGULATOR_CODE IN ('DE-BBK',
                                                                'UK-FSA',
                                                                'NL-DNB')
                        AND fctexpOTH.DAY_RK = VAR.rDay_rk
                        AND fctexpOTH.YEAR_MONTH_RK = VAR.rYear_Month_RK
                        AND (    (fctexpOTH.VALID_FLAG = 'Y')
                            AND (   (fctexpOTH.ADJUST_FLAG = 'U')
                                OR (fctexpOTH.ADJUST_FLAG = 'A')))
                        AND DIM_RRU.ll_rru IN ('NWBDEUAI', 'NWMDEUAI')
                        AND (CASE
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND fctexpOTH.ACLM_PRODUCT_TYPE IN ('BASEL-OTC',
                                                                        'REPO')
                                THEN
                                CASE
                                    WHEN     fctexpOTH.SYS_CODE IN ('SABFI2OR',
                                                                    'MUREX2OR',
                                                                    'MTH_OTH',
                                                                    'DAY_OTH')
                                        AND fctexpOTH.CAPITAL_CUTS = '4'
                                        AND '' = 'EU'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexpOTH.SYS_CODE,
                                                        'XX') NOT IN ('SABFI2OR',
                                                                    'MUREX2OR',
                                                                    'MTH_OTH',
                                                                    'DAY_OTH')
                                        AND (   fctexpOTH.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexpOTH.CAPITAL_CUTS =
                                                    ''
                                                OR LENGTH (
                                                    fctexpOTH.CAPITAL_CUTS) =
                                                    0)
                                        AND (   NULL
                                                    IS NULL
                                                OR '' =
                                                    ''
                                                )
                                    THEN
                                        'TRUE'
                                END
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND COALESCE (
                                            fctexpOTH.ACLM_PRODUCT_TYPE,
                                            'XX') NOT IN ('BASEL-OTC', 'REPO')
                                THEN
                                'TRUE'
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rdualrep_eff_from_yrmnth_rk
                                    AND VAR.rYear_Month_RK <
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                THEN
                                CASE
                                    WHEN     fctexpOTH.SYS_CODE IN ('SABFI2OR',
                                                                    'MUREX2OR')
                                        AND fctexpOTH.CRR2_FLAG = 'Y'
                                        AND fctexpOTH.CAPITAL_CUTS = '4'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexpOTH.SYS_CODE,
                                                        'XX') NOT IN ('SABFIMOR',
                                                                    'MUREXOR',
                                                                    'SABFI2OR',
                                                                    'MUREX2OR')
                                        AND COALESCE (fctexpOTH.CRR2_FLAG,
                                                        'N') <> 'Y'
                                        AND (   fctexpOTH.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexpOTH.CAPITAL_CUTS =
                                                    ''
                                                OR LENGTH (
                                                    fctexpOTH.CAPITAL_CUTS) =
                                                    0)
                                    THEN
                                        'TRUE'
                                END
                                WHEN VAR.rYear_Month_RK <
                                        VAR.rdualrep_eff_from_yrmnth_rk
                                THEN
                                'TRUE'
                            END = 'TRUE')
                UNION ALL
                SELECT NULL DEAL_ID_PREFIX,
                        fctexp.DEAL_ID,
                        fctexp.VALID_FLAG,
                        'CBI' REPORTING_REGULATOR_CODE,
                        fctexp.UNSETTLED_FACTOR_CODE,
                        fctexp.UNSETTLED_PERIOD_CODE,
                        'A' ADJUST_FLAG,
                        fctexp.REPORTING_TYPE_CODE,
                        fctexp.BANK_BASE_ROLE_CODE,
                        fctexp.ACCRUED_INT_ON_BAL_AMT,
                        fctexp.ACCRUED_INTEREST_GBP,
                        fctexp.APPROVED_APPROACH_CODE,
                        MOD.MODEL_CODE AS PD_MODEL_CODE,
                        dac.REG_EXPOSURE_SUB_TYPE
                        AS b2_irb_asset_class_reg_exposure_sub_type,
                        dac.BASEL_EXPOSURE_TYPE
                        AS b2_irb_asset_class_basel_exposure_type,
                        dac2.BASEL_EXPOSURE_TYPE
                        AS b2_std_asset_class_basel_exposure_type,
                        fctexp.B1_RWA_POST_CRM_AMT,
                        fctexp.B2_APP_ADJUSTED_LGD_RATIO,
                        fctexp.B2_APP_ADJUSTED_PD_RATIO,
                        fctexp.B2_APP_EAD_PRE_CRM_AMT,
                        fctexp.B2_IRB_EAD_PRE_CRM_AMT,
                        fctexp.B2_STD_EAD_PRE_CRM_AMT,
                        fctexp.B2_IRB_EAD_POST_CRM_AMT,
                        fctexp.B2_STD_EAD_POST_CRM_AMT,
                        fctexp.B2_IRB_RWA_POST_CRM_AMT,
                        fctexp.B2_APP_EAD_POST_CRM_AMT,
                        fctexp.B2_STD_RWA_POST_CRM_AMT,
                        fctexp.B2_APP_EXPECTED_LOSS_AMT,
                        fctexp.B2_IRB_NETTED_COLL_GBP,
                        fctexp.B2_APP_RISK_WEIGHT_RATIO,
                        fctexp.B2_APP_RWA_POST_CRM_AMT,
                        fctexp.B2_IRB_DEFAULT_DATE,
                        fctexp.B2_IRB_DRAWN_EAD,
                        fctexp.B2_IRB_DRAWN_EXPECTED_LOSS,
                        fctexp.B2_IRB_DRAWN_RWA,
                        fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS,
                        fctexp.B2_IRB_ORIG_EXP_PRE_CON_FACTOR,
                        fctexp.B2_IRB_UNDRAWN_EAD,
                        fctexp.B2_IRB_UNDRAWN_EXPECTED_LOSS,
                        fctexp.B2_IRB_UNDRAWN_RWA,
                        COALESCE (fctexp.B2_STD_CCF, 0) / 100 B2_STD_CCF,
                        fctexpREC.B2_STD_CCF_LESS_THAN_YEAR,
                        fctexpREC.B2_STD_CCF_MORE_THAN_YEAR,
                        fctexp.B2_STD_CREDIT_QUALITY_STEP,
                        fctexp.B2_STD_DEFAULT_DATE,
                        fctexp.B2_STD_DRAWN_EAD,
                        fctexp.B2_STD_DRAWN_RWA,
                        fctexp.B2_STD_EXP_HAIRCUT_ADJ_AMT,
                        fctexp.SYS_CODE,
                        CASE
                        WHEN COALESCE (fctexp.SYS_CODE, 'X') NOT IN ('ACBCNTNW',
                                                                        'ACBCNTRB',
                                                                        'LNIQNWTR',
                                                                        'LNIQRBTR')
                        THEN
                                COALESCE (fctexp.GROSS_DEP_UTILISATION_AMT,
                                        0)
                            - COALESCE (fctexp.NET_DEP_UTILISATION_AMT, 0)
                        ELSE
                            0
                        END
                        B2_STD_NETTED_COLL_GBP,
                        fctexp.GROSS_DEP_UTILISATION_AMT,
                        fctexp.net_dep_utilisation_amt,
                        fctexp.B2_STD_ORIG_EXP_PRE_CON_FACTOR,
                        fctexp.B2_STD_RISK_WEIGHT_RATIO,
                        fctexp.B2_STD_UNDRAWN_EAD,
                        fctexp.B2_STD_UNDRAWN_RWA,
                        fctexp.B3_CCP_MARGIN_N_INITL_RWA_AMT,
                        fctexp.B3_CCP_MARGIN_INITL_CASH_AMT,
                        fctexp.B3_CCP_MAR_INITL_NON_CASH_AMT,
                        fctexp.B3_CCP_MARGIN_INITL_RWA_AMT,
                        fctexp.B3_CVA_PROVISION_AMT,
                        fctexp.B3_CVA_PROVISION_RESIDUAL_AMT,
                        fctexp.JURISDICTION_FLAG,
                        fctexp.CAPITAL_CUTS,
                        CASE
                        WHEN     var.rYear_Month_RK >=
                                    var.rpradiv_eff_from_yrmnth_rk
                                AND fctexp.ACLM_PRODUCT_TYPE IN ('REPO',
                                                                'BASEL-OTC')
                        THEN
                            CASE
                                WHEN     fctexp.SYS_CODE IN ('SABFI2OR',
                                                            'MUREX2OR',
                                                            'MTH_OTH',
                                                            'DAY_OTH')
                                    AND fctexp.JURISDICTION_FLAG = 'EU'
                                    AND fctexp.CAPITAL_CUTS = '4'
                                THEN
                                    'Y'
                                ELSE
                                    fctexp.BASEL_DATA_FLAG
                            END
                        WHEN     var.rYear_Month_RK <
                                    var.rpradiv_eff_from_yrmnth_rk
                                AND var.rYear_Month_RK >=
                                    var.rdualrep_eff_from_yrmnth_rk
                        THEN
                            CASE
                                WHEN     fctexp.SYS_CODE IN ('SABFI2OR',
                                                            'MUREX2OR')
                                    AND fctexp.CRR2_FLAG = 'Y'
                                    AND fctexp.CAPITAL_CUTS = '4'
                                THEN
                                    'Y'
                                ELSE
                                    fctexp.BASEL_DATA_FLAG
                            END
                        ELSE
                            fctexp.BASEL_DATA_FLAG
                        END
                        BASEL_DATA_FLAG,
                        --fctexp.basel_data_flag,
                        fctexp.CRR2_FLAG,
                        fctexp.COLL_GOOD_PROV_AMT,
                        fctexp.COMM_LESS_THAN_1_YEAR,
                        fctexp.COMM_MORE_THAN_1_YEAR,
                        fctexp.CVA_CHARGE,
                        fctexp.IFRS9_PROV_IMPAIRMENT_AMT_GBP,
                        fctexp.IFRS9_PROV_WRITE_OFF_AMT_GBP,
                        fctexp.INTERNAL_TRANSACTION_FLAG,
                        fctexp.NET_EXPOSURE,
                        fctexp.NET_EXPOSURE_POST_SEC,
                        fctexp.OFF_BAL_LED_EXP_GBP_POST_SEC,
                        fctexp.OFF_BALANCE_EXPOSURE,
                        fctexp.ON_BAL_LED_EXP_GBP_POST_SEC,
                        fctexp.ON_BALANCE_LEDGER_EXPOSURE,
                        fctexp.PAST_DUE_ASSET_FLAG,
                        fctexp.PROVISION_AMOUNT_GBP,
                        fctexp.RESIDUAL_MATURITY_DAYS,
                        fctexp.RESIDUAL_VALUE,
                        fctexp.RETAIL_OFF_BAL_SHEET_FLAG,
                        fctexp.RISK_ON_GROUP_STRUCTURE,
                        fctexp.RISK_TAKER_GROUP_STRUCTURE,
                        fctexp.SECURITISED_FLAG,
                        fctexp.SET_OFF_INDICATOR,
                        fctexp.SRT_FLAG,
                        fctexp.TRADING_BOOK_FLAG,
                        fctexp.TYPE,
                        fctexp.UNDRAWN_COMMITMENT_AMT,
                        fctexp.UNDRAWN_COMMITMENT_AMT_PST_SEC,
                        fctexp.UNSETTLED_AMOUNT,
                        fctexp.UNSETTLED_PRICE_DIFFERENCE_AMT,
                        fctexp.LEASE_EXPOSURE_GBP,
                        fctexp.LEASE_PAYMENT_VALUE_AMT,
                        fctexp.OBLIGORS_COUNT,
                        fctexp.STD_EXPOSURE_DEFAULT_FLAG,
                        fctexp.CRD4_SOV_SUBSTITUTION_FLAG,
                        fctexp.UNLIKELY_PAY_FLAG,
                        fctexp.CRD4_STD_RP_SME_Flag,
                        fctexp.CRD4_IRB_RP_SME_Flag,
                        fctexp.B3_SME_DISCOUNT_FLAG,
                        fctexp.IRB_SME_DISCOUNT_APP_FLAG,
                        fctexp.TRANSITIONAL_PORTFOLIO_FLAG,
                        fctexp.FULLY_COMPLETE_SEC_FLAG,
                        fctexp.SAF_FILM_LEASING_FLAG,
                        fctexp.PRINCIPAL_AMT,
                        fctexp.MTM,
                        fctexp.AMORTISATION_AMT,
                        fct_count.BSD_MARKER,
                        dim_indust.UK_SIC_CODE,
                        fctexp.IMMOVABLE_PROP_INDICATOR,
                        fctexp.COREP_CRM_REPORTING_FLAG,
                        fctexp.QUAL_REV_EXP_FLAG,
                        fctexp.COMM_REAL_ESTATE_SEC_FLAG,
                        fctexpREC.MORTGAGES_FLAG,
                        fctexp.PERS_ACC_FLAG,
                        fctexp.CCP_RISK_WEIGHT,
                        fctexp.CS_PROXY_USED_FLAG,
                        fctexp.B2_IRB_INTEREST_AT_DEFAULT_GBP,
                        FCTEXP.REIL_AMT_GBP,
                        FCTEXP.REIL_STATUS,
                        fct_count.RP_TURNOVER_MAX_EUR,
                        fct_count.RP_TURNOVER_CCY,
                        fct_count.RP_SALES_TURNOVER_SOURCE,
                        fctexp.B3_SME_RP_FLAG,
                        fctexp.B3_SME_RP_IRB_CORP_FLAG,
                        fct_count.B3_SME_RP_1_5M_FLAG,
                        fct_count.RP_SALES_TURNOVER_LOCAL,
                        fct_count.TOTAL_ASSET_LOCAL,
                        fct_count.LE_TOTAL_ASSET_AMOUNT_EUR,
                        fctexp.MARKET_RISK_SUB_TYPE,
                        fctexp.LE_FI_AVC_CATEGORY,
                        fctexp.B2_IRB_RWA_R_COR_COEFF,
                        fctexp.B2_IRB_RWA_K_CAPITAL_REQ_PRE,
                        fctexp.B2_IRB_RWA_K_CAPITAL_REQ_POST,
                        fctexp.SCALE_UP_COEFENT_OF_CORELATION,
                        fctexp.B3_CVA_CC_ADV_FLAG,
                        dim_rru.LLP_RRU,
                        dim_rru.RRU_RK,
                        fctexp.B3_CVA_CC_ADV_VAR_AVG,
                        fctexp.B3_CVA_CC_ADV_VAR_SPOT,
                        fctexp.B3_CVA_CC_ADV_VAR_STRESS_AVG,
                        fctexp.B3_CVA_CC_ADV_VAR_STRESS_SPOT,
                        fctexp.B3_CVA_CC_CAPITAL_AMT,
                        fctexp.B3_CVA_CC_RWA_AMT,
                        fctexp.B3_CVA_CC_CDS_SNGL_NM_NTNL_AMT,
                        fctexp.B3_CVA_CC_CDS_INDEX_NTNL_AMT,
                        fctexp.B2_ALGO_IRB_CORP_SME_FLAG,
                        fctexp.STATUTORY_LEDGER_BALANCE,
                        fctexp.CARRYING_VALUE,
                        fctexp.LEVERAGE_EXPOSURE,
                        fct_count.STANDALONE_ENTITY,
                        fct_count.RP_TURNOVER_SOURCE_TYPE,
                        fctexp.IFRS9_FINAL_IIS_GBP,
                        fctexp.TOT_REGU_PROV_AMT_GBP,
                        fctexp.NET_MKT_VAL,
                        fctexp.B2_APP_CAP_REQ_POST_CRM_AMT,
                        fctexp.GRDW_POOL_ID,
                        fctexp.NEW_IN_DEFAULT_FLAG,
                        fctexp.RETAIL_POOL_SME_FLAG,
                        fctexp.GRDW_POOL_GROUP_ID,
                        COALESCE (fctexp.B2_IRB_CCF, 0) / 100 B2_IRB_CCF,
                        fctexp.DRAWN_LEDGER_BALANCE,
                        fctexp.E_STAR_GROSS_EXPOSURE,
                        fctexp.B2_IRB_RWA_PRE_CDS,
                        fctexp.LGD_CALC_MODE,
                        fctexp.DEFINITIVE_SLOT_CATEGORY_CALC,
                        fctexp.REPU_COMM_MORE_THAN_YEAR_GBP,
                        fctexp.REPU_COMM_LESS_THAN_YEAR_GBP,
                        fctexp.UNCOMM_UNDRAWN_AMT_GBP,
                        fctexp.TOTAL_UNDRAWN_AMOUNT_GBP,
                        fctexp.COLLECTIVELY_EVALUATED_FLAG,
                        fctexp.ORIGINAL_CURRENCY_CODE,
                        fctexp.ORIGINAL_TRADE_ID,
                        fctexp.E_STAR_NET_EXPOSURE,
                        fctexp.RISK_TAKER_RF_FLAG,
                        fctexp.RISK_TAKER_GS_CODE,
                        fctexp.RISK_ON_RF_FLAG,
                        fctexp.RISK_ON_GS_CODE,
                        fctexp.BORROWER_GS_CODE,
                        fctexp.STS_SECURITISATION,
                        fctexp.STS_SEC_QUAL_CAP_TRTMNT,
                        fctexp.STS_SEC_APPROACH_CODE,
                        fctexp.B3_APP_RWA_INC_CVA_CC_AMT,
                        fctexp.IFRS9_TOT_REGU_ECL_AMT_GBP,
                        fctexp.IFRS9_FINAL_ECL_MES_GBP,
                        fctexp.IFRS9_FINAL_STAGE,
                        fctexp.IFRS9_STAGE_3_TYPE,
                        fctexp.IFRS9_ECL_CURRENCY_CODE,
                        fctexp.IFRS9_TOT_STATUT_ECL_AMT_GBP,
                        fctexp.IFRS9_DISCNT_UNWIND_AMT_GBP,
                        fctexp.B1_EAD_POST_CRM_AMT,
                        fctexp.B1_RISK_WEIGHT_RATIO,
                        fctexp.B1_RWA_PRE_CRM_AMT,
                        fctexp.B1_UNDRAWN_COMMITMENT_GBP,
                        fctexp.B2_IRB_RWA_PRE_CRM_AMT,
                        fctexp.B2_STD_RWA_PRE_CRM_AMT,
                        fctexp.FACILITY_MATCHING_LEVEL,
                        fctexpREC.PRODUCT_FAMILY_CODE,
                        fctexp.CARTHESIS_REPORTING_ID,
                        fctexp.NGAAP_PROVISION_AMT_GBP,
                        dim_count.CIS_CODE AS EXTERNAL_COUNTERPARTY_ID_TEMP,
                        dim_count.RISK_ENTITY_CLASS
                        AS EXTERNAL_COUNTERPARTY_RISK_CLASS,
                        dim_count.QCCP AS External_COUNTERPARTY_QCCP_FLAG,
                        fctexp.SME_DISCOUNT_FACTOR,
                        fctexp.CRR2_501A_DISCOUNT_FLAG,
                        dac.REG_EXPOSURE_TYPE
                        AS b2_irb_asset_class_reg_exposure_type,
                        dac.basel_exposure_sub_type
                        AS b2_irb_asset_class_basel_exposure_sub_type,
                        dac2.basel_exposure_sub_type
                        AS b2_std_asset_class_basel_exposure_sub_type,
                        div.SUB2 AS DIVISION_SUB2,
                        div.DIVISION AS DIVISION,
                        dprod.REGULATORY_PRODUCT_TYPE
                        AS REGULATORY_PRODUCT_TYPE,
                        dim_wbu_parent.CIS_CODE AS ILE_COUNTERPARTY_CISCODE,
                        dim_wbu_parent.RISK_ENTITY_CLASS AS ILE_RISK_CLASS,
                        dim_parent.CIS_CODE AS BRANCH_CISCODE,
                        dim_parent.RISK_ENTITY_CLASS AS BRANCH_RISK_CLASS,
                        --'N' CRD4_REP_STDFLOW_FLAG,
                        CASE
                        WHEN div.SUB2 IN ('CITIZENS2001',
                                            'CITIZENS2002',
                                            'CITIZENS2003',
                                            'CITIZENS2001_NC',
                                            'CITIZENS2002_NC',
                                            'CITIZENS2003_NC')
                        THEN
                            'Y'
                        ELSE
                            'N'
                        END
                        CITIZENS_OVERRIDE_FLAG,
                        'N' CRD4_REP_STDFLOW_FLAG,
                        CASE
                        WHEN (CASE
                                    WHEN div.SUB2 IN ('CITIZENS2001',
                                                    'CITIZENS2002',
                                                    'CITIZENS2003',
                                                    'CITIZENS2001_NC',
                                                    'CITIZENS2002_NC',
                                                    'CITIZENS2003_NC')
                                    THEN
                                    'Y'
                                    ELSE
                                    'N'
                                END) = 'Y'
                        THEN
                            COALESCE (fctexp.B2_STD_EAD_POST_CRM_AMT, 0)
                        ELSE
                            COALESCE (fctexp.B2_APP_EAD_POST_CRM_AMT, 0)
                        END
                        EAD_POST_CRM_AMT,
                        CASE
                        WHEN     REPORTING_TYPE_CODE = 'NC'
                                AND dac2.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            'Other items'
                        ELSE
                            dac2.BASEL_EXPOSURE_TYPE
                        END
                        COREP_STD_BASEL_EXPOSURE_TYPE,
                        CASE
                        WHEN d_prod_cat.COREP_PRODUCT_CATEGORY_RK = -1
                        THEN
                            'UNKNOWN'
                        ELSE
                            d_prod_cat.COREP_PRODUCT_CATEGORY
                        END
                        AS COREP_PRODUCT_CATEGORY,
                        dim_indust.SECTOR_TYPE AS INDUSTRY_SECTOR_TYPE,
                        DIM_RRU.LL_RRU AS RISK_TAKER_LL_RRU,
                        dim_count.SOVEREIGN_REGIONAL_GOVT_FLAG
                        AS SOVEREIGN_REGIONAL_GOVT_FLAG,
                        MOD2.MODEL_CODE AS EAD_MODEL_CODE,
                        dim_rru2.ll_rru AS RISK_ON_LL_RRU,
                        CASE
                        WHEN     dac3.basel_exposure_type = 'Retail'
                                AND dac3.basel_exposure_sub_type = 'N/A'
                        THEN
                            CASE
                                WHEN fctexp.QUAL_REV_EXP_FLAG = 'Y'
                                THEN
                                    'Qualifying Revolving Exposures'
                                ELSE
                                    CASE
                                    WHEN    fctexp.COMM_REAL_ESTATE_SEC_FLAG =
                                                'Y'
                                            OR fctexpREC.MORTGAGES_FLAG = 'Y'
                                    THEN
                                        CASE
                                            WHEN fctexp.PERS_ACC_FLAG = 'N'
                                            THEN
                                                'Secured by Real Estate Property - SME'
                                            ELSE
                                                'Secured by Real Estate Property - Non-SME'
                                        END
                                    ELSE
                                        CASE
                                            WHEN fctexp.PERS_ACC_FLAG = 'N'
                                            THEN
                                                'Other Retail Exposures - SME'
                                            ELSE
                                                'Other Retail Exposures - Non-SME'
                                        END
                                    END
                            END
                        ELSE
                            dac3.basel_exposure_sub_type
                        END
                        COREP_IRB_EXPOSURE_SUB_TYPE,
                        UPPER (fctexp.ACLM_PRODUCT_TYPE) AS ACLM_PRODUCT_TYPE,
                        d_prod_cat2.ACLM_PRODUCT_TYPE
                        AS ACLM_PRODUCT_TYPE_d_prod_cat2,
                        (SELECT PaRAM_VALUE
                        FROM {scdl_db}.dim_rwa_engine_generic_params
                        WHERE param_code = 'INFRADISCOUNTFACTOR')
                        AS v_infra_discount_factor,
                        CASE
                        WHEN     fctexp.REPORTING_TYPE_CODE = 'NC'
                                AND dac5.BASEL_EXPOSURE_TYPE NOT IN ('Equity Claims',
                                                                    'Exposures to central governments or central banks')
                        THEN
                            var.v_oth_itm_asset_class_rk
                        ELSE
                            fctexp.B2_STD_ASSET_CLASS_RK
                        END
                        COREP_STD_ASSET_CLASS_RK,
                        dac.TYPE AS dac_type,
                        dac.valid_flag AS dac_valid_flag,
                        dac.asset_class_rk AS dac_asset_class_rk,
                        dac4.BASEL_EXPOSURE_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE,
                        dce.P3_COUNTRY_DESC AS INCORP_P3_COUNTRY_DESC,
                        dcc2.COST_CENTRE,
                        dce2.P3_COUNTRY_DESC AS OP_P3_COUNTRY_DESC,
                        'EXPOSURE' FLOW_TYPE,
                        COALESCE (fctexp.IFRS9_PROV_WRITE_OFF_AMT_GBP, 0)
                        IMP_WRITE_OFF_AMT,
                        CASE
                        WHEN fctexp.B3_CVA_CC_RWA_AMT IS NOT NULL THEN 'Y'
                        ELSE 'N'
                        END
                        CRD4_REP_CVA_FLAG,
                        IFRS9_BSPL_800_MAP.IAS39_CLASS,
                        CASE
                        WHEN (   dim_count.CIS_CODE = 'Z6VR0IT'
                                OR fctexp.DEAL_ID LIKE 'AR2956%')
                        THEN
                            'OTHER'
                        ELSE
                            'IFRS9'
                        END
                        REP_PROVISION_TYPE,
                        CASE
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexp.DEAL_ID LIKE '%_SI%' 
                        THEN
                            'SI'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexp.DEAL_ID LIKE '%_DTA%' 
                        THEN
                            'DT'
                        WHEN     d_prod_cat2.ACLM_PRODUCT_TYPE = 'NCA'
                                AND fctexp.DEAL_ID LIKE '%_NSI%' 
                        THEN
                            'NSI'
                        ELSE
                            NULL
                        END
                        DEAL_TYPE,
                        CASE
                        WHEN fctexp.APPROVED_APPROACH_CODE = 'STD'
                        THEN
                            fctexp.B3_SME_DISCOUNT_FLAG
                        ELSE
                            NULL
                        END
                        STD_SME_DISCOUNT_APP_FLAG,
                        DIM_MATURITY.MATURITY_BAND_DESC,
                        CASE
                        WHEN fctexp.REPORTING_TYPE_CODE = 'SR' THEN 'Y'
                        ELSE 'N'
                        END
                        CRD4_REP_SETTL_FLAG,
                        dim_count.FI_AVC_CATEGORY,
                        d_prod_cat2.COREP_PRODUCT_CATEGORY
                        COREP_EXPOSURE_CATEGORY,
                        pd_band.mgs AS dim_mgs,
                        pd_band.pd_band_code AS dim_pd_band_code,
                        CASE
                        WHEN dim_indust.UK_SIC_CODE = 'UNKNOWN' THEN NULL
                        ELSE dim_indust.UK_SIC_CODE
                        END
                        EC92_CODE,
                        dim_ind.sector_cluster,
                        dim_ind.SUB_SECTOR,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS MEMO_B2_STD_BASEL_EXPOSURE_TYPE,
                        GL_MASTER.BSPL_900_GL_CODE AS BSPL_900_GL_CODE,
                        GL_MASTER.BSPL_60_GL_CODE AS BSPL_60_GL_CODE,
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                                AS rprov_eff_from_year_month
                        FROM {rtd_ref_db}.etl_parameter
                        WHERE parameter_name = 'DE_BBK_IFRS9_PROV_EFF_DATE')
                        AS rprov_eff_from_year_month,
                        dprod.aiml_product_type AS aiml_product_type,
                        dce.P3_COUNTRY_DESC AS INCORP_COUNTRY_DESC,
                        dce2.P3_COUNTRY_DESC AS OP_COUNTRY_DESC,
                        COALESCE (dce.p3_country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_p3_country_desc,
                        COALESCE (dce.country_desc,
                                'Unknown Geographic Area')
                        AS corep_incorp_country_desc,
                        DIM_RRU.LL_RRU_DESC AS RISK_TAKER_LL_RRU_DESC,
                        DIM_RRU2.LL_RRU_DESC AS RISK_ON_LL_RRU_DESC,
                        dim_counter.cis_code AS LE,
                        dim_count.SYS_CODE AS CSYS_CODE,
                        dim_count.SYS_ID AS CSYS_ID,
                        DIM_IND.SECTOR_TYPE,
                        DIM_IND.SECTOR,
                        dprod.HIGH_PROD,
                        dim_parent.cis_code AS parent_cis_code,
                        dac5.BASEL_EXPOSURE_TYPE
                        AS B2_STD_BASEL_EXPOSURE_TYPE,
                        dim_rule.RULE_DESC,
                        dac4.BASEL_EXPOSURE_SUB_TYPE
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE,
                        dac4.reg_exposure_type
                        AS B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE,
                        dac4.TYPE AS B2_STD_PRE_DFLT_ASSET_CLASS_TYPE,
                        dcc2.cost_centre_type,
                        dac5.basel_exposure_sub_type
                        AS MEMO_B2_STD_BASEL_EXPOSURE_SUB_TYPE,
                        dac5.TYPE AS MEMO_B2_STD_TYPE,
                        dac5.reg_exposure_type
                        AS memo_b2_std_reg_exposure_type,
                        -- fctexp.snap_dt ,
                        --fctexp.dataload_time AS original_time,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name = 'ICB_EFFECTIVE_DATE') AS INTEGER)
                        AS v_icb_eff_from_year_month_rk,
                        CAST (
                        (SELECT date_format (to_date (parameter_value,'dd-MMM-yyyy'),'yyyyMM')
                            FROM {rtd_ref_db}.etl_parameter
                            WHERE parameter_name =
                                    'CORE_TO_CORE_IG_EXP_EFF_DATE') AS INTEGER)
                        AS v_core_ig_eff_year_month_rk,
                        fctexp.YEAR_MONTH_RK AS YEAR_MONTH,
                        COALESCE (
                        COALESCE (
                            (CASE
                                WHEN dim_wbu_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_wbu_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_parent.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_parent.counterparty_rk
                            END),
                            (CASE
                                WHEN dim_count.counterparty_rk = -1
                                THEN
                                    NULL
                                ELSE
                                    dim_count.counterparty_rk
                            END)),
                        -1)
                        AS LER_CHILD_COUNTERPARTY,
                        fctexp.day_rk,
                        CASE
                        WHEN     fctexp.REPORTING_REGULATOR_CODE = 'CBI'
                                AND fctexp.B2_APP_ADJUSTED_PD_RATIO = 1
                                AND dac.REG_EXPOSURE_SUB_TYPE =
                                    'Income Producing Real Estate'
                                AND dac.BASEL_EXPOSURE_TYPE = 'Corporate'
                                AND fctexp.DEFINITIVE_SLOT_CATEGORY_CALC =
                                    'N'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN dprod.regulatory_product_type = 'PR_EQ_CIU'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN MOD.model_code = 'SRW'
                        THEN
                            'AIRB - Equity Simple Risk Weight'
                        WHEN MOD.model_code = 'SG'
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Object Finance',
                                                                'Commodities Finance')
                        THEN
                            'AIRB - SL PD/LGD Approach'
                        WHEN     dac.reg_exposure_type = 'Corporates'
                                AND dac.reg_exposure_sub_type IN ('Income Producing Real Estate',
                                                                'Project Finance')
                        THEN
                            'AIRB - SL Slotting Approach'
                        ELSE
                            'AIRB - PD/LGD Approach'
                        END
                        RWA_CALC_METHOD,
                        fctexp.b2_irb_asset_class_rk,
                        fctexp.rru_rk AS risk_taker_rru,
                        fctexp.risk_on_rru_rk AS risk_on_rru,
                        CASE
                        WHEN COALESCE (fctexp.B2_APP_EAD_POST_CRM_AMT, 0) =
                                0
                        THEN
                            'NULL EAD'
                        ELSE
                            CASE
                                WHEN fctexp.COUNTERPARTY_RK = -1
                                THEN
                                    'RETAIL'
                                ELSE
                                    'CORP'
                            END
                        END
                        OBLIGOR_TYPE,
                        div.SUB1,
                        div.SUB1_DESC,
                        div.SUB2,
                        div.SUB2_DESC,
                        div.SUB3,
                        div.SUB3_DESC,
                        div.SUB4,
                        div.SUB4_DESC,
                        div.SUB5,
                        div.SUB5_DESC,
                        div.DIVISION_DESC
                FROM {scdl_db}.FCT_EXPOSURES fctexp
                        JOIN VAR ON VAR.rYear_Month_RK = fctexp.YEAR_MONTH_RK
                        LEFT OUTER JOIN {scdl_db}.FCT_EXPOSURES_REC fctexpREC
                        ON (    fctexp.DEAL_ID = fctexpREC.DEAL_ID
                            AND fctexp.DAY_RK = fctexpREC.DAY_RK
                            AND fctexp.YEAR_MONTH_RK =
                                    fctexpREC.YEAR_MONTH_RK
                            AND fctexp.VALID_FLAG = fctexpREC.VALID_FLAG)
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU dim_rru
                        ON (    fctexp.rru_rk = dim_rru.rru_rk
                            AND dim_rru.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_rru.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat
                        ON ( (CASE
                                    WHEN UPPER (fctexp.ACLM_PRODUCT_TYPE) =
                                            'N/A'
                                    THEN
                                    'UNKNOWN'
                                    ELSE
                                    UPPER (fctexp.ACLM_PRODUCT_TYPE)
                                END) = d_prod_cat.ACLM_PRODUCT_TYPE)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES fct_count
                        ON (    fctexp.COUNTERPARTY_RK =
                                    fct_count.COUNTERPARTY_RK
                            AND fctexp.DAY_RK = fct_count.DAY_RK
                            AND fctexp.YEAR_MONTH_RK =
                                    fct_count.YEAR_MONTH_RK
                            AND fct_count.VALID_FLAG = 'Y')
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY dim_indust
                        ON (    fct_count.EC92_INDUSTRY_RK =
                                    dim_indust.INDUSTRY_RK
                            AND dim_indust.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_indust.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac
                        ON (    fctexp.B2_IRB_ASSET_CLASS_RK =
                                    dac.ASSET_CLASS_RK
                            AND dac.valid_from_date <= var.v_reporting_date
                            AND dac.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac2
                        ON (    fctexp.B2_STD_ASSET_CLASS_RK =
                                    dac2.ASSET_CLASS_RK
                            AND dac2.TYPE = 'B2_STD'
                            AND dac2.BASEL_EXPOSURE_TYPE = 'Other items'
                            AND dac2.valid_from_date <= var.v_reporting_date
                            AND dac2.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_DIVISION div
                        ON (    fctexp.DIVISION_RK = div.DIVISION_RK
                            AND div.valid_from_date <= var.v_reporting_date
                            AND div.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL MOD
                        ON (    fctexp.PD_MODEL_RK = MOD.MODEL_RK
                            AND MOD.valid_from_date <= var.v_reporting_date
                            AND MOD.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_PRODUCT dprod
                        ON (    fctexp.PRODUCT_RK = dprod.PRODUCT_RK
                            AND dprod.valid_from_date <=
                                    var.v_reporting_date
                            AND dprod.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.dim_cost_centre dc
                        ON (    fctexp.COST_CENTRE_RK = dc.COST_CENTRE_RK
                            AND dc.valid_from_date <= var.v_reporting_date
                            AND dc.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN counterparty_data dim_count
                        ON (    fctexp.COUNTERPARTY_RK =
                                    dim_count.COUNTERPARTY_RK
                            AND dim_count.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_count.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN dcc dcc1
                        ON (dim_count.counterparty_rk =
                                dcc1.wbu_counterparty_rk)
                        LEFT JOIN counterparty_data dim_wbu_parent
                        ON (    dim_wbu_parent.counterparty_rk =
                                    dcc1.le_counterparty_rk
                            AND dim_wbu_parent.group_internal_flag = 'Y'
                            AND dim_wbu_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_wbu_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT JOIN counterparty_data dim_parent
                        ON (    dim_parent.counterparty_rk =
                                    fct_count.branch_parent_counterparty_rk
                            AND dim_parent.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_parent.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_MODEL MOD2
                        ON     fctexp.EAD_MODEL_RK = MOD2.MODEL_RK
                            AND MOD2.valid_from_date <= var.v_reporting_date
                            AND MOD2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RRU DIM_RRU2
                        ON     fctexp.RISK_ON_RRU_RK = dim_rru2.rru_rk
                            AND DIM_RRU2.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_RRU2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac3
                        ON     fctexp.COREP_ASSET_CLASS_RK =
                                    dac3.ASSET_CLASS_RK
                            AND dac3.valid_from_date <= var.v_reporting_date
                            AND dac3.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN
                        {scdl_db}.DIM_COREP_PRODUCT_CATEGORY d_prod_cat2
                        ON fctexp.COREP_PRODUCT_CATEGORY_RK =
                                d_prod_cat2.COREP_PRODUCT_CATEGORY_RK
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac4
                        ON     fctexp.B2_STD_PRE_DFLT_ASSET_CLASS_RK =
                                    dac4.ASSET_CLASS_RK
                            AND dac4.valid_from_date <= var.v_reporting_date
                            AND dac4.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce
                        ON     fctexp.INCORP_COUNTRY_RK = dce.COUNTRY_RK
                            AND dce.valid_from_date <= var.v_reporting_date
                            AND dce.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_COST_CENTRE dcc2
                        ON (    fctexp.COST_CENTRE_RK =
                                    dcc2.COST_CENTRE_RK
                            AND dcc2.valid_flag = 'Y'
                            AND dcc2.valid_from_date <= var.v_reporting_date
                            AND dcc2.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED dce2
                        ON     fctexp.OP_COUNTRY_RK = dce2.COUNTRY_RK
                            AND dce2.valid_from_date <= var.v_reporting_date
                            AND dce2.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.GL_MASTER GL_MASTER
                        ON (    fctexp.GL_ACCOUNT_RK = GL_MASTER.GL_RK
                            AND GL_MASTER.valid_from_date <=
                                    var.v_reporting_date
                            AND GL_MASTER.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.IFRS9_BSPL_800_MAP IFRS9_BSPL_800_MAP
                        ON (GL_MASTER.BSPL_800_GL_CODE =
                                IFRS9_BSPL_800_MAP.BSPL800_CODE)
                        LEFT OUTER JOIN {scdl_db}.DIM_MATURITY_BAND DIM_MATURITY
                        ON (    (   (    (fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS) >
                                            DIM_MATURITY.LOWER_VALUE
                                        AND (fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS) <
                                            DIM_MATURITY.UPPER_VALUE)
                                    OR (    fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.LOWER_VALUE
                                        AND DIM_MATURITY.LOWER_VAL_INCLUSIVE =
                                            'Y')
                                    OR (    fctexp.B2_IRB_EFFECTIVE_MATURITY_YRS =
                                            DIM_MATURITY.UPPER_VALUE
                                        AND DIM_MATURITY.UPPER_VAL_INCLUSIVE =
                                            'Y'))
                            AND DIM_MATURITY.TYPE = 'S'
                            AND DIM_MATURITY.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_MATURITY.valid_to_date >
                                    var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_PD_BAND pd_band
                        ON (    (   (    COALESCE (
                                            fctexp.B2_APP_ADJUSTED_PD_RATIO,
                                            0.02501) > pd_band.lower_value
                                        AND COALESCE (
                                            fctexp.B2_APP_ADJUSTED_PD_RATIO,
                                            0.02501) < pd_band.upper_value)
                                    OR (    COALESCE (
                                            fctexp.B2_APP_ADJUSTED_PD_RATIO,
                                            0.02501) = pd_band.lower_value
                                        AND pd_band.lower_val_inclusive = 'Y')
                                    OR (    COALESCE (
                                            fctexp.B2_APP_ADJUSTED_PD_RATIO,
                                            0.02501) = pd_band.upper_value
                                        AND pd_band.upper_val_inclusive = 'Y'))
                            AND pd_band.TYPE = 'MGS'
                            AND pd_band.valid_from_date <=
                                    var.v_reporting_date
                            AND pd_band.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {scdl_db}.DIM_INDUSTRY DIM_IND
                        ON (    fctexp.INDUSTRY_RK = DIM_IND.INDUSTRY_RK
                            AND DIM_IND.valid_from_date <=
                                    var.v_reporting_date
                            AND DIM_IND.valid_to_date > var.v_reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_ASSET_CLASS dac5
                        ON     fctexp.B2_STD_ASSET_CLASS_RK =
                                    dac5.ASSET_CLASS_RK
                            AND dac5.valid_from_date <= var.v_reporting_date
                            AND dac5.valid_to_date > var.v_reporting_date
                        LEFT OUTER JOIN counterparty_data dim_counter
                        ON     dcc2.LE_COUNTERPARTY_RK =
                                    dim_counter.counterparty_rk
                            AND dim_counter.valid_from_date <=
                                    var.v_reporting_date
                            AND dim_counter.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_op_country
                        ON     fctexp.OP_COUNTRY_RK =
                                    d_op_country.COUNTRY_RK
                            AND d_op_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_op_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {rtd_ref_db}.DIM_COUNTRY_ENRICHED d_incorp_country
                        ON     fctexp.INCORP_COUNTRY_RK =
                                    d_incorp_country.COUNTRY_RK
                            AND d_incorp_country.valid_from_date <=
                                    var.v_reporting_date
                            AND d_incorp_country.valid_to_date >
                                    var.v_reporting_date
                        LEFT OUTER JOIN {scdl_db}.DIM_RULE dim_rule
                        ON fctexp.STD_RW_PRIORITY_ORDER_RULE_RK =
                                dim_rule.RULE_RK
                WHERE     fctexp.DAY_RK = VAR.rDay_rk
                        AND fctexp.YEAR_MONTH_RK = VAR.rYear_Month_RK
                        AND (    (fctexp.VALID_FLAG = 'Y')
                            AND (   (fctexp.ADJUST_FLAG = 'U')
                                OR (fctexp.ADJUST_FLAG = 'A')))
                        AND dim_rru.LLP_RRU LIKE '%UBIL%'
                        AND (CASE
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND fctexp.ACLM_PRODUCT_TYPE IN ('BASEL-OTC',
                                                                    'REPO')
                                THEN
                                CASE
                                    WHEN     fctexp.SYS_CODE IN ('SABFI2OR',
                                                                'MUREX2OR',
                                                                'MTH_OTH',
                                                                'DAY_OTH')
                                        AND fctexp.CAPITAL_CUTS = '4'
                                        AND JURISDICTION_FLAG = 'EU'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexp.SYS_CODE,
                                                        'XX') NOT IN ('SABFI2OR',
                                                                    'MUREX2OR',
                                                                    'MTH_OTH',
                                                                    'DAY_OTH')
                                        AND (   fctexp.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexp.CAPITAL_CUTS = ''
                                                OR LENGTH (
                                                    fctexp.CAPITAL_CUTS) =
                                                    0)
                                        AND (   fctexp.JURISDICTION_FLAG
                                                    IS NULL
                                                OR fctexp.JURISDICTION_FLAG =
                                                    ''
                                                OR LENGTH (
                                                    fctexp.JURISDICTION_FLAG) =
                                                    0)
                                    THEN
                                        'TRUE'
                                END
                                WHEN     VAR.rYear_Month_RK >=
                                            VAR.rpradiv_eff_from_yrmnth_rk
                                    AND COALESCE (fctexp.ACLM_PRODUCT_TYPE,
                                                'XX') NOT IN ('BASEL-OTC',
                                                                'REPO')
                                THEN
                                'TRUE'
                                WHEN VAR.rYear_Month_RK <
                                        VAR.rpradiv_eff_from_yrmnth_rk
                                THEN
                                CASE
                                    WHEN     fctexp.SYS_CODE IN ('SABFI2OR',
                                                                'MUREX2OR')
                                        AND fctexp.CRR2_FLAG = 'Y'
                                        AND fctexp.CAPITAL_CUTS = '4'
                                    THEN
                                        'TRUE'
                                    WHEN     COALESCE (fctexp.SYS_CODE,
                                                        'XX') NOT IN ('SABFIMOR',
                                                                    'MUREXOR',
                                                                    'SABFI2OR',
                                                                    'MUREX2OR')
                                        AND COALESCE (fctexp.CRR2_FLAG,
                                                        'N') <> 'Y'
                                        AND (   fctexp.CAPITAL_CUTS
                                                    IS NULL
                                                OR fctexp.CAPITAL_CUTS = ''
                                                OR LENGTH (
                                                    fctexp.CAPITAL_CUTS) =
                                                    0)
                                    THEN
                                        'TRUE'
                                END
                            END = 'TRUE')"""
    
    stg1 = spark.sql(stg1_query);
    stg1.createOrReplaceTempView("stg1");

    print("\n\nTotal records fetched in first stage of exposures : ", stg1.count());
    print("\n\n");

    stg_query = f""" SELECT stg.DEAL_ID_PREFIX,
            stg.DEAL_ID,
            stg.VALID_FLAG,
            stg.REPORTING_REGULATOR_CODE,
            stg.UNSETTLED_FACTOR_CODE,
            stg.UNSETTLED_PERIOD_CODE,
            stg.ADJUST_FLAG,
            stg.BANK_BASE_ROLE_CODE,
            stg.ACCRUED_INT_ON_BAL_AMT,
            stg.ACCRUED_INTEREST_GBP,
            stg.APPROVED_APPROACH_CODE,
            stg.PD_MODEL_CODE,
            stg.b2_irb_asset_class_reg_exposure_sub_type,
            stg.b2_irb_asset_class_basel_exposure_type,
            stg.b2_std_asset_class_basel_exposure_type,
            stg.B1_RWA_POST_CRM_AMT,
            stg.B2_APP_ADJUSTED_LGD_RATIO,
            stg.B2_APP_ADJUSTED_PD_RATIO,
            stg.B2_APP_EAD_PRE_CRM_AMT,
            stg.B2_IRB_EAD_PRE_CRM_AMT,
            stg.B2_STD_EAD_PRE_CRM_AMT,
            stg.B2_IRB_EAD_POST_CRM_AMT,
            stg.B2_STD_EAD_POST_CRM_AMT,
            stg.B2_IRB_RWA_POST_CRM_AMT,
            stg.B2_APP_EAD_POST_CRM_AMT,
            stg.B2_STD_RWA_POST_CRM_AMT,
            stg.B2_APP_EXPECTED_LOSS_AMT,
            stg.B2_IRB_NETTED_COLL_GBP,
            stg.B2_APP_RISK_WEIGHT_RATIO,
            stg.B2_APP_RWA_POST_CRM_AMT,
            stg.B2_IRB_DEFAULT_DATE,
            stg.B2_IRB_DRAWN_EAD,
            stg.B2_IRB_DRAWN_EXPECTED_LOSS,
            stg.B2_IRB_DRAWN_RWA,
            stg.B2_IRB_EFFECTIVE_MATURITY_YRS,
            stg.B2_IRB_ORIG_EXP_PRE_CON_FACTOR,
            stg.B2_IRB_UNDRAWN_EAD,
            stg.B2_IRB_UNDRAWN_EXPECTED_LOSS,
            stg.B2_IRB_UNDRAWN_RWA,
            stg.B2_STD_CCF,
            stg.B2_STD_CCF_LESS_THAN_YEAR,
            stg.B2_STD_CCF_MORE_THAN_YEAR,
            stg.B2_STD_CREDIT_QUALITY_STEP,
            stg.B2_STD_DEFAULT_DATE,
            stg.B2_STD_DRAWN_EAD,
            stg.B2_STD_DRAWN_RWA,
            stg.B2_STD_EXP_HAIRCUT_ADJ_AMT,
            stg.SYS_CODE,
            stg.B2_STD_NETTED_COLL_GBP,
            stg.GROSS_DEP_UTILISATION_AMT,
            stg.net_dep_utilisation_amt,
            stg.B2_STD_ORIG_EXP_PRE_CON_FACTOR,
            stg.B2_STD_RISK_WEIGHT_RATIO,
            stg.B2_STD_UNDRAWN_EAD,
            stg.B2_STD_UNDRAWN_RWA,
            stg.B3_CCP_MARGIN_N_INITL_RWA_AMT,
            stg.B3_CCP_MARGIN_INITL_CASH_AMT,
            stg.B3_CCP_MAR_INITL_NON_CASH_AMT,
            stg.B3_CCP_MARGIN_INITL_RWA_AMT,
            stg.B3_CVA_PROVISION_AMT,
            stg.B3_CVA_PROVISION_RESIDUAL_AMT,
            stg.JURISDICTION_FLAG,
            stg.CAPITAL_CUTS,
            stg.BASEL_DATA_FLAG,
            stg.CRR2_FLAG,
            stg.COLL_GOOD_PROV_AMT,
            stg.COMM_LESS_THAN_1_YEAR,
            stg.COMM_MORE_THAN_1_YEAR,
            stg.CVA_CHARGE,
            stg.IFRS9_PROV_IMPAIRMENT_AMT_GBP,
            stg.IFRS9_PROV_WRITE_OFF_AMT_GBP,
            stg.INTERNAL_TRANSACTION_FLAG,
            stg.NET_EXPOSURE,
            stg.NET_EXPOSURE_POST_SEC,
            stg.OFF_BAL_LED_EXP_GBP_POST_SEC,
            stg.OFF_BALANCE_EXPOSURE,
            stg.ON_BAL_LED_EXP_GBP_POST_SEC,
            stg.ON_BALANCE_LEDGER_EXPOSURE,
            stg.PAST_DUE_ASSET_FLAG,
            stg.PROVISION_AMOUNT_GBP,
            stg.RESIDUAL_MATURITY_DAYS,
            stg.RESIDUAL_VALUE,
            stg.RETAIL_OFF_BAL_SHEET_FLAG,
            stg.RISK_ON_GROUP_STRUCTURE,
            stg.RISK_TAKER_GROUP_STRUCTURE,
            stg.SECURITISED_FLAG,
            stg.SET_OFF_INDICATOR,
            stg.SRT_FLAG,
            stg.TRADING_BOOK_FLAG,
            stg.TYPE,
            stg.UNDRAWN_COMMITMENT_AMT,
            stg.UNDRAWN_COMMITMENT_AMT_PST_SEC,
            stg.UNSETTLED_AMOUNT,
            stg.UNSETTLED_PRICE_DIFFERENCE_AMT,
            stg.LEASE_EXPOSURE_GBP,
            stg.LEASE_PAYMENT_VALUE_AMT,
            stg.OBLIGORS_COUNT,
            stg.STD_EXPOSURE_DEFAULT_FLAG,
            stg.CRD4_SOV_SUBSTITUTION_FLAG,
            stg.UNLIKELY_PAY_FLAG,
            stg.CRD4_STD_RP_SME_Flag,
            stg.CRD4_IRB_RP_SME_Flag,
            stg.B3_SME_DISCOUNT_FLAG,
            stg.IRB_SME_DISCOUNT_APP_FLAG,
            stg.TRANSITIONAL_PORTFOLIO_FLAG,
            stg.FULLY_COMPLETE_SEC_FLAG,
            stg.SAF_FILM_LEASING_FLAG,
            stg.PRINCIPAL_AMT,
            stg.MTM,
            stg.AMORTISATION_AMT,
            stg.BSD_MARKER,
            stg.UK_SIC_CODE,
            stg.IMMOVABLE_PROP_INDICATOR,
            stg.COREP_CRM_REPORTING_FLAG,
            stg.QUAL_REV_EXP_FLAG,
            stg.COMM_REAL_ESTATE_SEC_FLAG,
            stg.MORTGAGES_FLAG,
            stg.PERS_ACC_FLAG,
            stg.CCP_RISK_WEIGHT,
            stg.CS_PROXY_USED_FLAG,
            stg.B2_IRB_INTEREST_AT_DEFAULT_GBP,
            stg.REIL_AMT_GBP,
            stg.REIL_STATUS,
            stg.RP_TURNOVER_MAX_EUR,
            stg.RP_TURNOVER_CCY,
            stg.RP_SALES_TURNOVER_SOURCE,
            stg.B3_SME_RP_FLAG,
            stg.B3_SME_RP_IRB_CORP_FLAG,
            stg.B3_SME_RP_1_5M_FLAG,
            stg.RP_SALES_TURNOVER_LOCAL,
            stg.TOTAL_ASSET_LOCAL,
            stg.LE_TOTAL_ASSET_AMOUNT_EUR,
            stg.MARKET_RISK_SUB_TYPE,
            stg.LE_FI_AVC_CATEGORY,
            stg.B2_IRB_RWA_R_COR_COEFF,
            stg.B2_IRB_RWA_K_CAPITAL_REQ_PRE,
            stg.B2_IRB_RWA_K_CAPITAL_REQ_POST,
            stg.SCALE_UP_COEFENT_OF_CORELATION,
            stg.B3_CVA_CC_ADV_FLAG,
            stg.LLP_RRU,
            stg.RRU_RK,
            stg.B3_CVA_CC_ADV_VAR_AVG,
            stg.B3_CVA_CC_ADV_VAR_SPOT,
            stg.B3_CVA_CC_ADV_VAR_STRESS_AVG,
            stg.B3_CVA_CC_ADV_VAR_STRESS_SPOT,
            stg.B3_CVA_CC_CAPITAL_AMT,
            stg.B3_CVA_CC_RWA_AMT,
            stg.B3_CVA_CC_CDS_SNGL_NM_NTNL_AMT,
            stg.B3_CVA_CC_CDS_INDEX_NTNL_AMT,
            stg.B2_ALGO_IRB_CORP_SME_FLAG,
            stg.STATUTORY_LEDGER_BALANCE,
            stg.CARRYING_VALUE,
            stg.LEVERAGE_EXPOSURE,
            stg.STANDALONE_ENTITY,
            stg.RP_TURNOVER_SOURCE_TYPE,
            stg.IFRS9_FINAL_IIS_GBP,
            stg.TOT_REGU_PROV_AMT_GBP,
            stg.NET_MKT_VAL,
            stg.B2_APP_CAP_REQ_POST_CRM_AMT,
            stg.GRDW_POOL_ID,
            stg.NEW_IN_DEFAULT_FLAG,
            stg.RETAIL_POOL_SME_FLAG,
            stg.GRDW_POOL_GROUP_ID,
            stg.B2_IRB_CCF,
            stg.DRAWN_LEDGER_BALANCE,
            stg.E_STAR_GROSS_EXPOSURE,
            stg.B2_IRB_RWA_PRE_CDS,
            stg.LGD_CALC_MODE,
            stg.DEFINITIVE_SLOT_CATEGORY_CALC,
            stg.REPU_COMM_MORE_THAN_YEAR_GBP,
            stg.REPU_COMM_LESS_THAN_YEAR_GBP,
            stg.UNCOMM_UNDRAWN_AMT_GBP,
            stg.TOTAL_UNDRAWN_AMOUNT_GBP,
            stg.COLLECTIVELY_EVALUATED_FLAG,
            stg.ORIGINAL_CURRENCY_CODE,
            stg.ORIGINAL_TRADE_ID,
            stg.E_STAR_NET_EXPOSURE,
            stg.RISK_TAKER_RF_FLAG,
            stg.RISK_TAKER_GS_CODE,
            stg.RISK_ON_RF_FLAG,
            stg.RISK_ON_GS_CODE,
            stg.BORROWER_GS_CODE,
            stg.STS_SECURITISATION,
            stg.STS_SEC_QUAL_CAP_TRTMNT,
            stg.STS_SEC_APPROACH_CODE,
            stg.B3_APP_RWA_INC_CVA_CC_AMT,
            stg.IFRS9_TOT_REGU_ECL_AMT_GBP,
            stg.IFRS9_FINAL_ECL_MES_GBP,
            stg.IFRS9_FINAL_STAGE,
            stg.IFRS9_STAGE_3_TYPE,
            stg.IFRS9_ECL_CURRENCY_CODE,
            stg.IFRS9_TOT_STATUT_ECL_AMT_GBP,
            stg.IFRS9_DISCNT_UNWIND_AMT_GBP,
            stg.B1_EAD_POST_CRM_AMT,
            stg.B1_RISK_WEIGHT_RATIO,
            stg.B1_RWA_PRE_CRM_AMT,
            stg.B1_UNDRAWN_COMMITMENT_GBP,
            stg.B2_IRB_RWA_PRE_CRM_AMT,
            stg.B2_STD_RWA_PRE_CRM_AMT,
            stg.FACILITY_MATCHING_LEVEL,
            stg.PRODUCT_FAMILY_CODE,
            stg.CARTHESIS_REPORTING_ID,
            stg.NGAAP_PROVISION_AMT_GBP,
            stg.EXTERNAL_COUNTERPARTY_ID_TEMP,
            stg.EXTERNAL_COUNTERPARTY_RISK_CLASS,
            stg.External_COUNTERPARTY_QCCP_FLAG,
            stg.SME_DISCOUNT_FACTOR,
            stg.CRR2_501A_DISCOUNT_FLAG,
            stg.b2_irb_asset_class_reg_exposure_type,
            stg.b2_irb_asset_class_basel_exposure_sub_type,
            stg.b2_std_asset_class_basel_exposure_sub_type,
            stg.DIVISION_SUB2,
            stg.DIVISION,
            stg.REGULATORY_PRODUCT_TYPE,
            stg.ILE_COUNTERPARTY_CISCODE,
            stg.ILE_RISK_CLASS,
            stg.BRANCH_CISCODE,
            stg.BRANCH_RISK_CLASS,
            stg.CITIZENS_OVERRIDE_FLAG,
            stg.CRD4_REP_STDFLOW_FLAG,
            stg.EAD_POST_CRM_AMT,
            stg.COREP_STD_BASEL_EXPOSURE_TYPE,
            stg.COREP_PRODUCT_CATEGORY,
            stg.INDUSTRY_SECTOR_TYPE,
            stg.RISK_TAKER_LL_RRU,
            stg.SOVEREIGN_REGIONAL_GOVT_FLAG,
            stg.EAD_MODEL_CODE,
            stg.RISK_ON_LL_RRU,
            stg.COREP_IRB_EXPOSURE_SUB_TYPE,
            stg.ACLM_PRODUCT_TYPE,
            stg.ACLM_PRODUCT_TYPE_d_prod_cat2,
            stg.v_infra_discount_factor,
            stg.COREP_STD_ASSET_CLASS_RK,
            stg.dac_type,
            stg.dac_valid_flag,
            stg.dac_asset_class_rk,
            stg.B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE,
            stg.INCORP_P3_COUNTRY_DESC,
            stg.COST_CENTRE,
            stg.OP_P3_COUNTRY_DESC,
            stg.FLOW_TYPE,
            stg.IMP_WRITE_OFF_AMT,
            stg.CRD4_REP_CVA_FLAG,
            stg.IAS39_CLASS,
            stg.REP_PROVISION_TYPE,
            stg.DEAL_TYPE,
            stg.STD_SME_DISCOUNT_APP_FLAG,
            stg.MATURITY_BAND_DESC,
            stg.CRD4_REP_SETTL_FLAG,
            stg.FI_AVC_CATEGORY,
            stg.COREP_EXPOSURE_CATEGORY,
            stg.dim_mgs,
            stg.dim_pd_band_code,
            stg.EC92_CODE,
            stg.sector_cluster,
            stg.SUB_SECTOR,
            stg.MEMO_B2_STD_BASEL_EXPOSURE_TYPE,
            stg.BSPL_900_GL_CODE,
            stg.BSPL_60_GL_CODE,
            stg.rprov_eff_from_year_month,
            stg.aiml_product_type,
            stg.INCORP_COUNTRY_DESC,
            stg.OP_COUNTRY_DESC,
            stg.corep_incorp_p3_country_desc,
            stg.corep_incorp_country_desc,
            stg.RISK_TAKER_LL_RRU_DESC,
            stg.RISK_ON_LL_RRU_DESC,
            stg.LE,
            stg.CSYS_CODE,
            stg.CSYS_ID,
            stg.SECTOR_TYPE,
            stg.SECTOR,
            stg.HIGH_PROD,
            stg.parent_cis_code,
            stg.B2_STD_BASEL_EXPOSURE_TYPE,
            stg.RULE_DESC,
            stg.B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE,
            stg.B2_STD_PRE_DFLT_ASSET_CLASS_REG_EXPOSURE_TYPE,
            stg.B2_STD_PRE_DFLT_ASSET_CLASS_TYPE,
            stg.cost_centre_type,
            stg.MEMO_B2_STD_BASEL_EXPOSURE_SUB_TYPE,
            stg.MEMO_B2_STD_TYPE,
            stg.memo_b2_std_reg_exposure_type,
            stg.v_icb_eff_from_year_month_rk,
            stg.v_core_ig_eff_year_month_rk,
            stg.LER_CHILD_COUNTERPARTY,
            d_ctpty.RISK_ENTITY_CLASS AS dim_ctrpty_risk_entity_class,
            ovm_excl.OVERRIDE_VALUE,
            stg.day_rk,
            stg.RWA_CALC_METHOD,
            COALESCE (
                (SELECT first(corep_irb.asset_class_rk)
                    FROM {srtd_db}.dim_asset_class corep_irb
                    WHERE     corep_irb.basel_exposure_type =
                                dim_asset_class2.basel_exposure_type
                        AND corep_irb.basel_exposure_sub_type =
                                CASE
                                    WHEN dim_asset_class2.REG_EXPOSURE_TYPE IN ('Central Governments and Central Banks',
                                                                                'Institutions',
                                                                                'Non-credit Obligation Assets',
                                                                                'Equities',
                                                                                'Securitisation Positions')
                                    THEN
                                    corep_irb.basel_exposure_sub_type
                                    WHEN     dim_asset_class2.REG_EXPOSURE_TYPE =
                                                'Corporates'
                                        AND stg.RWA_CALC_METHOD =
                                                'AIRB - SL PD/LGD Approach'
                                    THEN
                                    'RWA Calc Under AIRB - SL PD/LGD'
                                    WHEN     dim_asset_class2.REG_EXPOSURE_TYPE =
                                                'Corporates'
                                        AND stg.RWA_CALC_METHOD =
                                                'AIRB - SL Slotting Approach'
                                    THEN
                                    'RWA Calc Under AIRB - SL Slotting'
                                    WHEN dim_asset_class2.REG_EXPOSURE_TYPE =
                                            'Retail'
                                    THEN
                                    stg.COREP_IRB_EXPOSURE_SUB_TYPE
                                    ELSE
                                    dim_asset_class2.basel_exposure_sub_type
                                END
                        AND corep_irb.TYPE = 'COREP_IRB'
                        AND corep_irb.valid_flag = 'Y'),
                -1)
                COREP_IRB_ASSET_CLASS_RK,
                (SELECT first(REG_EXPOSURE_TYPE)
                FROM {srtd_db}.dim_asset_class corep_irb
                WHERE     corep_irb.basel_exposure_type =
                            dim_asset_class2.basel_exposure_type
                    AND corep_irb.basel_exposure_sub_type =
                            CASE
                                WHEN dim_asset_class2.REG_EXPOSURE_TYPE IN ('Central Governments and Central Banks',
                                                                            'Institutions',
                                                                            'Non-credit Obligation Assets',
                                                                            'Equities',
                                                                            'Securitisation Positions')
                                THEN
                                    corep_irb.basel_exposure_sub_type
                                WHEN     dim_asset_class2.REG_EXPOSURE_TYPE =
                                            'Corporates'
                                    AND stg.RWA_CALC_METHOD =
                                            'AIRB - SL PD/LGD Approach'
                                THEN
                                    'RWA Calc Under AIRB - SL PD/LGD'
                                WHEN     dim_asset_class2.REG_EXPOSURE_TYPE =
                                            'Corporates'
                                    AND stg.RWA_CALC_METHOD =
                                            'AIRB - SL Slotting Approach'
                                THEN
                                    'RWA Calc Under AIRB - SL Slotting'
                                WHEN stg.TYPE = 'RET'
                                THEN
                                    stg.COREP_IRB_EXPOSURE_SUB_TYPE
                                ELSE
                                    dim_asset_class2.basel_exposure_sub_type
                            END
                    AND corep_irb.TYPE = 'COREP_IRB'
                    AND corep_irb.valid_flag = 'Y' )
                AS CRD4_REP_IRBEQ_REG_EXPOSURE_TYPE,
            stg.B2_IRB_ASSET_CLASS_RK,
            stg.risk_taker_rru,
            stg.risk_on_rru,
            stg.OBLIGOR_TYPE,
            stg.SUB1,
            stg.SUB1_DESC,
            stg.SUB2,
            stg.SUB2_DESC,
            stg.SUB3,
            stg.SUB3_DESC,
            stg.SUB4,
            stg.SUB4_DESC,
            stg.SUB5,
            stg.SUB5_DESC,
            stg.DIVISION_DESC,
            stg.YEAR_MONTH,
            stg.REPORTING_TYPE_CODE
        FROM stg1 stg
            JOIN var ON (VAR.rYear_Month_RK = stg.YEAR_MONTH)
            LEFT OUTER JOIN counterparty_data d_ctpty
                ON (stg.LER_CHILD_COUNTERPARTY = d_ctpty.counterparty_rk)
            LEFT OUTER JOIN {srtd_db}.OVERRIDE_MASTER ovm_excl
                ON (    ovm_excl.overridden_value = d_ctpty.cis_code
                    AND ovm_excl.override_rule = 'EXC_IG_CIS'
                    AND ovm_excl.override_reason = 'CIS EXCLUSION'
                    AND ovm_excl.valid_from_date <= var.v_reporting_date
                    AND ovm_excl.valid_to_date > var.v_reporting_date)
            JOIN {srtd_db}.dim_asset_class dim_asset_class2
                ON (stg.b2_irb_asset_class_rk =
                        dim_asset_class2.asset_class_rk)"""

    final_exp_stg_df = spark.sql(stg_query);
    
    print('\n\nTotal records in exposures_staging : ', final_exp_stg_df.count());
    print("\n\n");
	#preprod_output_location = 's3://bucket-eu-west-1-978523670193-processed-data-s/presentation/rtd/srtd_mart/rwa_reporting_exposures_staging'
    preprod_output_location = f"s3://bucket-eu-west-1-{account_id}-processed-data-s/presentation/rtd/srtd_mart/rwa_reporting_exposures_staging/YEAR_MONTH={year_month}/"

    #final_exp_stg_df.write.format("parquet").mode("overwrite").insertInto(srtd_mart_db + '.' + stg_exp_tab_name, overwrite = True)
    #final_exp_stg_df.write.option("path", 
     #                           f"{preprod_output_location}").mode("overwrite").partitionBy("year_month",
    #                            "reporting_regulator_code").saveAsTable(srtd_mart_db + '.' + stg_exp_tab_name);
	final_exp_stg_df.repartition(1).write.mode("overwrite").partitionBy("reporting_regulator_code").save(preprod_output_location);

	print('\n\n\n\n**************FINISHED EXPOSURES*********************\n\n\n\n')
#   end of populate_exp_staging()


def populate_sub_exp_staging(spark, reporting_date, scdl_db, srtd_db, srtd_mart_db, stg_sub_exp_tab_name):
    var_query = f"""SELECT date_format('{reporting_date}', 'yyyyMM') rYear_Month_RK ,
                        date_format('{reporting_date}', 'yyyyMMdd') rDay_RK,
                        date ('{reporting_date}') AS reporting_date""";
    var_df = spark.sql(var_query);
    var_df.createOrReplaceTempView("var");
    
    t2_query = f"""SELECT fctSbExp.DEAL_ID,
                        fctSbExp.VALID_FLAG,
                        fctSbExp.MITIGANT_ID,
                        fctMtgnts.SYS_CODE,
                        fctMtgnts.ADJUST_FLAG,
                        fctSbExp.COREP_CRM_CATEGORY,
                        fctSbExp.B2_IRB_COLL_AMT,
                        fctSbExp.B2_IRB_GTEE_AMT,
                        coalesce(fctSbExp.B2_STD_EFF_GTEE_AMT, 0) B2_STD_EFF_GTEE_AMT,
                        coalesce(fctSbExp.B2_STD_EFF_CDS_AMT, 0) B2_STD_EFF_CDS_AMT,
                        fctSbExp.B2_STD_ELIG_COLL_AMT,
                        fctSbExp.B2_STD_EFF_COLL_AMT_POST_HCUT,
                        fctSbExp.B2_STD_COLL_VOLATILITY_AMT,
                        fctSbExp.B2_STD_COLL_MTY_MISMATCH_AMT,
                        fctSbExp.B2_STD_GTOR_RW_RATIO,
                        fctSbExp.B2_STD_GTEE_CDS_RWA,
                        fctSbExp.B2_STD_FX_HAIRCUT,
                        fctSbExp.TYPE,
                        fctSbExp.B2_IRB_COLLTRL_AMT_TTC,
                        fctSbExp.B2_IRB_GARNTEE_AMT_TTC,
                        fctSbExp.SEC_DISCNT_B2_IRB_COLL_AMT_TTC,
                        fctSbExp.SEC_DISCNT_B2_IRB_GTEE_AMT_TTC,
                        fctSbExp.DOUBLE_DEFAULT_FLAG,
                        fctSbExp.SEC_DISCNTD_B2_IRB_GARNTEE_AMT,
                        fctSbExp.SEC_DISCNTD_B2_IRB_COLLTRL_AMT,
                        fctMtgnts.SYS_PROTECTION_TYPE,
                        fctMtgnts.BASEL_SECURITY_SUB_TYPE,
                        fct.B2_STD_ASSET_CLASS_RK     
                        AS PROVIDER_B2_STD_ASSET_CLASS,
                        dmt.MITIGANT_TYPE,
                        d_coll_type.COLLATERAL_TYPE_CODE,
                        'INFLOW' FLOW_TYPE,
                        COALESCE(fctc.B2_STD_ASSET_CLASS_RK, -1) AS COREP_STD_ASSET_CLASS_RK,
                        fctSbExp.YEAR_MONTH_RK AS YEAR_MONTH,
                        'UK-FSA' AS REPORTING_REGULATOR_CODE
                FROM {scdl_db}.FCT_SUB_EXPOSURES fctSbExp
                    INNER JOIN VAR on var.rYear_Month_RK = fctSbExp.YEAR_MONTH_RK
                        INNER JOIN {scdl_db}.FCT_MITIGANTS fctMtgnts
                        ON (    fctSbExp.MITIGANT_ID =
                                    fctMtgnts.MITIGANT_ID
                            AND fctSbExp.VALID_FLAG = 'Y'
                            AND fctMtgnts.VALID_FLAG = 'Y')
                        INNER JOIN {scdl_db}.DIM_MITIGANT_TYPE dmt
                        ON (fctSbExp.MITIGANT_TYPE_RK =
                                dmt.MITIGANT_TYPE_RK
                                and dmt.valid_from_date <=  var.reporting_date 
                                and dmt.valid_to_date >  var.reporting_date)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES FCT
                        ON (    fctSbExp.PROVIDER_COUNTERPARTY_RK =
                                    FCT.COUNTERPARTY_RK
                            AND FCT.DAY_RK = var.rDay_RK 
                            AND fctSbExp.YEAR_MONTH_RK = FCT.YEAR_MONTH_RK
                            --- AND FCT.VALID_FLAG = 'Y'
                            and FCT.valid_from_date <=  var.reporting_date 
                                and FCT.valid_to_date >  var.reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_COLLATERAL_TYPE d_coll_type
                        on( fctSbExp.COLLATERAL_TYPE_RK = d_coll_type.COLLATERAL_TYPE_RK
                        and d_coll_type.valid_from_date <=  var.reporting_date 
                                and d_coll_type.valid_to_date >  var.reporting_date)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES FCTC
                        ON (    fctSbExp.PROVIDER_COUNTERPARTY_RK =
                                    FCTC.COUNTERPARTY_RK
                            AND fctSbExp.YEAR_MONTH_RK =FCTC.YEAR_MONTH_RK
                            AND FCTC.DAY_RK = rDay_RK -- RANG-03592:10-Feb-2014:Added
                            AND FCTC.VALID_FLAG = 'Y') 
                WHERE     fctSbExp.DAY_RK = var.rDay_RK
                    
                        AND fctSbExp.YEAR_MONTH_RK = var.rYear_Month_RK
                        AND fctMtgnts.DAY_RK = var.rDay_RK
                        AND fctMtgnts.YEAR_MONTH_RK = var.rYear_Month_RK
                        AND fctSbExp.VALID_FLAG = 'Y'
                        AND fctMtgnts.VALID_FLAG = 'Y'
                                                
            UNION ALL                                             
            SELECT fctSbExpOTH.DEAL_ID,
                        fctSbExpOTH.VALID_FLAG,
                        fctSbExpOTH.MITIGANT_ID,
                        fctMtgnts.SYS_CODE,
                        fctMtgnts.ADJUST_FLAG,
                        fctSbExpOTH.COREP_CRM_CATEGORY,
                        fctSbExpOTH.B2_IRB_COLL_AMT,
                        fctSbExpOTH.B2_IRB_GTEE_AMT,
                        coalesce(fctSbExpOTH.B2_STD_EFF_GTEE_AMT, 0 ) B2_STD_EFF_GTEE_AMT,
                        coalesce(fctSbExpOTH.B2_STD_EFF_CDS_AMT, 0) B2_STD_EFF_CDS_AMT,
                        fctSbExpOTH.B2_STD_ELIG_COLL_AMT,
                        fctSbExpOTH.B2_STD_EFF_COLL_AMT_POST_HCUT,
                        fctSbExpOTH.B2_STD_COLL_VOLATILITY_AMT,
                        fctSbExpOTH.B2_STD_COLL_MTY_MISMATCH_AMT,
                        fctSbExpOTH.B2_STD_GTOR_RW_RATIO,
                        fctSbExpOTH.B2_STD_GTEE_CDS_RWA,
                        fctSbExpOTH.B2_STD_FX_HAIRCUT,
                        fctSbExpOTH.TYPE,
                        fctSbExpOTH.B2_IRB_COLLTRL_AMT_TTC,
                        fctSbExpOTH.B2_IRB_GARNTEE_AMT_TTC,
                        fctSbExpOTH.SEC_DISCNT_B2_IRB_COLL_AMT_TTC,
                        fctSbExpOTH.SEC_DISCNT_B2_IRB_GTEE_AMT_TTC,
                        fctSbExpOTH.DOUBLE_DEFAULT_FLAG,
                        fctSbExpOTH.SEC_DISCNTD_B2_IRB_GARNTEE_AMT,
                        fctSbExpOTH.SEC_DISCNTD_B2_IRB_COLLTRL_AMT,
                        fctMtgnts.SYS_PROTECTION_TYPE,
                        fctMtgnts.BASEL_SECURITY_SUB_TYPE,
                        fctSbExpOTH.B2_STD_ASSET_CLASS_RK
                        AS PROVIDER_B2_STD_ASSET_CLASS,
                        dmt.MITIGANT_TYPE,
                        d_coll_type.COLLATERAL_TYPE_CODE,
                        'INFLOW' FLOW_TYPE,
                        COALESCE(fctc.B2_STD_ASSET_CLASS_RK, -1) AS COREP_STD_ASSET_CLASS_RK,
                        fctSbExpOTH.YEAR_MONTH_RK AS YEAR_MONTH,
                        fctSbExpOTH.REPORTING_REGULATOR_CODE
                    FROM {scdl_db}.FCT_SUB_EXPOSURES_OTH fctSbExpOTH
                    INNER JOIN VAR on var.rYear_Month_RK = fctSbExpOTH.YEAR_MONTH_RK
                            INNER JOIN {scdl_db}.FCT_MITIGANTS fctMtgnts
                            ON (    fctSbExpOTH.MITIGANT_ID =
                                        fctMtgnts.MITIGANT_ID
                                AND fctSbExpOTH.VALID_FLAG = 'Y'
                                AND fctMtgnts.VALID_FLAG = 'Y')
                        INNER JOIN {scdl_db}.DIM_MITIGANT_TYPE dmt
                        ON (fctSbExpOTH.MITIGANT_TYPE_RK =
                                dmt.MITIGANT_TYPE_RK
                                and dmt.valid_from_date <=  var.reporting_date 
                                and dmt.valid_to_date >  var.reporting_date)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES FCT
                        ON (    fctSbExpOTH.PROVIDER_COUNTERPARTY_RK =
                                    FCT.COUNTERPARTY_RK
                            AND FCT.DAY_RK = var.rDay_RK 
                            AND fctSbExpOTH.YEAR_MONTH_RK = FCT.YEAR_MONTH_RK
                            --- AND FCT.VALID_FLAG = 'Y'
                            and FCT.valid_from_date <=  var.reporting_date 
                                and FCT.valid_to_date >  var.reporting_date)
                        LEFT OUTER JOIN {srtd_db}.DIM_COLLATERAL_TYPE d_coll_type
                        on( fctSbExpOTH.COLLATERAL_TYPE_RK = d_coll_type.COLLATERAL_TYPE_RK
                        and d_coll_type.valid_from_date <=  var.reporting_date 
                                and d_coll_type.valid_to_date >  var.reporting_date)
                        LEFT OUTER JOIN {scdl_db}.FCT_COUNTERPARTIES FCTC
                        ON (    fctSbExpOTH.PROVIDER_COUNTERPARTY_RK =
                                    FCTC.COUNTERPARTY_RK
                            AND fctSbExpOTH.YEAR_MONTH_RK =
                                    FCTC.YEAR_MONTH_RK
                            AND FCTC.DAY_RK = rDay_RK 
                            AND FCTC.VALID_FLAG = 'Y')
                WHERE     fctSbExpOTH.REPORTING_REGULATOR_CODE = 'DE-BBK'
                    
                        AND fctSbExpOTH.DAY_RK = var.rDay_RK
                        AND fctSbExpOTH.YEAR_MONTH_RK = var.rYear_Month_RK
                        AND fctMtgnts.DAY_RK = var.rDay_RK
                        AND fctMtgnts.YEAR_MONTH_RK = var.rYear_Month_RK""";
    t2_df = spark.sql(t2_query);
    t2_df.createOrReplaceTempView("t2");

    print("\n\nTotal records fetched in t2 stage of sub-exposures : ", t2_df.count());
    print("\n\n");
    
    t3_query = f"""SELECT t2.DEAL_ID,
                        t2.VALID_FLAG,
                        t2.MITIGANT_ID,
                        t2.SYS_CODE,
                        t2.ADJUST_FLAG,
                        t2.COREP_CRM_CATEGORY,
                        t2.B2_IRB_COLL_AMT,
                        t2.B2_IRB_GTEE_AMT,
                        t2.B2_STD_EFF_GTEE_AMT,
                        t2.B2_STD_EFF_CDS_AMT,
                        t2.B2_STD_ELIG_COLL_AMT,
                        t2.B2_STD_EFF_COLL_AMT_POST_HCUT,
                        t2.B2_STD_COLL_VOLATILITY_AMT,
                        t2.B2_STD_COLL_MTY_MISMATCH_AMT,
                        t2.B2_STD_GTOR_RW_RATIO,
                        t2.B2_STD_GTEE_CDS_RWA,
                        t2.B2_STD_FX_HAIRCUT,
                        t2.TYPE,
                        t2.B2_IRB_COLLTRL_AMT_TTC,
                        t2.B2_IRB_GARNTEE_AMT_TTC,
                        t2.SEC_DISCNT_B2_IRB_COLL_AMT_TTC,
                        t2.SEC_DISCNT_B2_IRB_GTEE_AMT_TTC,
                        t2.DOUBLE_DEFAULT_FLAG,
                        t2.SEC_DISCNTD_B2_IRB_GARNTEE_AMT,
                        t2.SEC_DISCNTD_B2_IRB_COLLTRL_AMT,
                        t2.SYS_PROTECTION_TYPE,
                        t2.BASEL_SECURITY_SUB_TYPE,
                        t2.PROVIDER_B2_STD_ASSET_CLASS,
                        t2.MITIGANT_TYPE,
                        t2.COLLATERAL_TYPE_CODE,
                        t2.FLOW_TYPE,
                        t2.COREP_STD_ASSET_CLASS_RK,
                        t2.YEAR_MONTH,
                        'DE-BBK' REPORTING_REGULATOR_CODE
                FROM t2,var
                WHERE     t2.YEAR_MONTH = var.rYear_Month_RK
                        AND t2.REPORTING_REGULATOR_CODE = 'UK-FSA'                              
                    UNION ALL
                
                SELECT t2.DEAL_ID,
                        t2.VALID_FLAG,
                        t2.MITIGANT_ID,
                        t2.SYS_CODE,
                        t2.ADJUST_FLAG,
                        t2.COREP_CRM_CATEGORY,
                        t2.B2_IRB_COLL_AMT,
                        t2.B2_IRB_GTEE_AMT,
                        t2.B2_STD_EFF_GTEE_AMT,
                        t2.B2_STD_EFF_CDS_AMT,
                        t2.B2_STD_ELIG_COLL_AMT,
                        t2.B2_STD_EFF_COLL_AMT_POST_HCUT,
                        t2.B2_STD_COLL_VOLATILITY_AMT,
                        t2.B2_STD_COLL_MTY_MISMATCH_AMT,
                        t2.B2_STD_GTOR_RW_RATIO,
                        t2.B2_STD_GTEE_CDS_RWA,
                        t2.B2_STD_FX_HAIRCUT,
                        t2.TYPE,
                        t2.B2_IRB_COLLTRL_AMT_TTC,
                        t2.B2_IRB_GARNTEE_AMT_TTC,
                        t2.SEC_DISCNT_B2_IRB_COLL_AMT_TTC,
                        t2.SEC_DISCNT_B2_IRB_GTEE_AMT_TTC,
                        t2.DOUBLE_DEFAULT_FLAG,
                        t2.SEC_DISCNTD_B2_IRB_GARNTEE_AMT,
                        t2.SEC_DISCNTD_B2_IRB_COLLTRL_AMT,
                        t2.SYS_PROTECTION_TYPE,
                        t2.BASEL_SECURITY_SUB_TYPE,
                        t2.PROVIDER_B2_STD_ASSET_CLASS,
                        t2.MITIGANT_TYPE,
                        t2.COLLATERAL_TYPE_CODE,
                        t2.FLOW_TYPE,
                        t2.COREP_STD_ASSET_CLASS_RK,
                        t2.YEAR_MONTH,
                        'NL-DNB' REPORTING_REGULATOR_CODE
                FROM t2,var
                WHERE     t2.YEAR_MONTH = var.rYear_Month_RK
                        AND t2.REPORTING_REGULATOR_CODE = 'UK-FSA'
                
                UNION ALL
                SELECT t2.DEAL_ID,
                        t2.VALID_FLAG,
                        t2.MITIGANT_ID,
                        t2.SYS_CODE,
                        t2.ADJUST_FLAG,
                        t2.COREP_CRM_CATEGORY,
                        t2.B2_IRB_COLL_AMT,
                        t2.B2_IRB_GTEE_AMT,
                        t2.B2_STD_EFF_GTEE_AMT,
                        t2.B2_STD_EFF_CDS_AMT,
                        t2.B2_STD_ELIG_COLL_AMT,
                        t2.B2_STD_EFF_COLL_AMT_POST_HCUT,
                        t2.B2_STD_COLL_VOLATILITY_AMT,
                        t2.B2_STD_COLL_MTY_MISMATCH_AMT,
                        t2.B2_STD_GTOR_RW_RATIO,
                        t2.B2_STD_GTEE_CDS_RWA,
                        t2.B2_STD_FX_HAIRCUT,
                        t2.TYPE,
                        t2.B2_IRB_COLLTRL_AMT_TTC,
                        t2.B2_IRB_GARNTEE_AMT_TTC,
                        t2.SEC_DISCNT_B2_IRB_COLL_AMT_TTC,
                        t2.SEC_DISCNT_B2_IRB_GTEE_AMT_TTC,
                        t2.DOUBLE_DEFAULT_FLAG,
                        t2.SEC_DISCNTD_B2_IRB_GARNTEE_AMT,
                        t2.SEC_DISCNTD_B2_IRB_COLLTRL_AMT,
                        t2.SYS_PROTECTION_TYPE,
                        t2.BASEL_SECURITY_SUB_TYPE,
                        t2.PROVIDER_B2_STD_ASSET_CLASS,
                        t2.MITIGANT_TYPE,
                        t2.COLLATERAL_TYPE_CODE,
                        t2.FLOW_TYPE,
                        t2.COREP_STD_ASSET_CLASS_RK,
                        t2.YEAR_MONTH,
                        'CBI' REPORTING_REGULATOR_CODE
                FROM t2,var
                WHERE     t2.YEAR_MONTH = var.rYear_Month_RK
                        AND t2.REPORTING_REGULATOR_CODE = 'UK-FSA'""";
    t3_df = spark.sql(t3_query);
    t3_df.createOrReplaceTempView("t3");
    
    print("\n\nTotal records fetched in t3 stage of sub-exposures : ", t3_df.count());
    print("\n\n");

    final_sub_exp_stg_df = t2_df.unionByName(t3_df);
    print('\n\nTotal records in sub_exposures_staging : ', final_sub_exp_stg_df.count());
    print("\n\n");
    
    output_tbl_name = srtd_mart_db + '.' + stg_sub_exp_tab_name;

    print('\n\nWriting data for sub exposure staging here : ', output_tbl_name);
    print("\n\n");
	#preprod_output_location1 = 's3://bucket-eu-west-1-978523670193-processed-data-s/presentation/rtd/srtd_mart/rwa_reporting_sub_exposures_staging'
    preprod_output_location1 = f"s3://bucket-eu-west-1-{account_id}-processed-data-s/presentation/rtd/srtd_mart/rwa_reporting_sub_exposures_staging/YEAR_MONTH={year_month}/"

    #final_sub_exp_stg_df.write.format("parquet").mode("overwrite").insertInto(output_tbl_name, overwrite = True);
    #final_sub_exp_stg_df.write.option("path", 
     #                           f"{preprod_output_location1}").mode("overwrite").partitionBy("year_month",
    #                            "reporting_regulator_code").saveAsTable(output_tbl_name);                                
    final_sub_exp_stg_df.repartition(1).write.mode("overwrite").partitionBy("reporting_regulator_code").save(preprod_output_location1);
	print('\n\n\n\n**************FINISHED SUB  EXPOSURES*********************\n\n\n\n')
#   end of populate_sub_exp_staging()


if __name__ == "__main__":
    
    print("\n\n\n***********Inside Main method***********")
    
    spark = get_spark_session();
    config = read_config(spark);
	account_id = str(popen("curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .accountId").read().strip())


    print("\n\nParameters from config file\n\n");
	#p_reporting_date = '2022-06-30'
    p_reporting_date = config["Input_parameters"]["reporting_date"];
    reporting_date = datetime.strptime(p_reporting_date, '%Y-%m-%dT%H::%M::%S.%f').strftime('%Y-%m-%d');
    srtd_mart_db = config["Database_details"]["srtd_mart_db"];
    rtd_ref_db =  config["Database_details"]["rtd_ref_db"];
    frank_db = config["Database_details"]["frank_db"];
    sfrank_db = config["Database_details"]["sfrank_db"];
    wsls_db = config["Database_details"]["wslstaging_db"];
	
    #initializing the databases
    srtd_db = frank_db
    scdl_db = sfrank_db
    stg_exp_tab_name = "rwa_reporting_exposures_staging"
    stg_sub_exp_tab_name = "rwa_reporting_sub_exposures_staging"

    print("\nreporting_date : ", reporting_date);
    print("\nsrtd_mart_db : ", srtd_mart_db);
    print("\nrtd_ref_db : ", rtd_ref_db);
    print("\nfrank_db : ", frank_db);
    print("\nsfrank_db : ", sfrank_db);
    print("\nwsls_db : ", wsls_db);
    print("\nsrtd_db : ", srtd_db);
    print("\nscdl_db : ", scdl_db);
    print("\nstg_exp_tab_name : ", stg_exp_tab_name);
    print("\nstg_sub_exp_tab_name : ", stg_sub_exp_tab_name);

    print('''\n\n\n\nLoading Exposures Staging Table''')
    populate_exp_staging(spark, reporting_date, scdl_db, srtd_db, wsls_db, rtd_ref_db, srtd_mart_db, stg_exp_tab_name);

    print('''\n\n\nLoading Sub Exposures Staging Table''')
    populate_sub_exp_staging(spark, reporting_date, scdl_db, srtd_db, srtd_mart_db, stg_sub_exp_tab_name);

    print("\n\n**************Interface Tables Refreshed****************\n\n")
#   end of main()