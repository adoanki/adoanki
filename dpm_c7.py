from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import lit,expr,col,when,substring_index,round
from pyspark.sql.types import StringType
from datetime import datetime

# Imports from S3
from modules.load_fct_dpm_return import load_fct_dpm_return
from modules.common_function import get_spark_session
from modules.common_function import get_run_rk
from modules.common_function import get_return_rk
from modules.common_function import read_config

###################################################################################################################
############# This function will create a temp view `input_df_view` on the spark session            ###############
#############    that will be used in gathering attributes for create_int_agg_df                    ###############
#############    This function is called inside prepare_3_0_v1_c07_00 function.                     ###############
###################################################################################################################
def get_input_data_for_int_agg_df(spark, rtd_mart_db, srtd_mart_db, frank_db,v_year_month_rk, p_reporting_date,rtd_ref_db):

    query = f"""
        SELECT
            fct.B2_STD_EXPOSURE_VALUE,
            fct.B2_STD_FULLY_ADJ_EXP_CCF_50,
            fct.B2_STD_FULLY_ADJ_EXP_CCF_20,
            fct.B2_STD_FULLY_ADJ_EXP_CCF_0,
            fct.B2_STD_EFF_FIN_COLL_AMT,
            fct.B2_STD_NETTED_COLL_GBP,
            fct.B2_STD_CRM_TOT_OUTFLOW,
            fct.B2_STD_CRM_TOT_INFLOW,
            fct.B2_STD_PROVISIONS_AMT,
            fct.B2_STD_EXPOSURE_VALUE_ONBAL,
            fct.COREP_PRODUCT_CATEGORY,
            fct.B2_STD_CRM_TOT_OUTFLOW_ONBAL,
            fct.B2_STD_CRM_TOT_INFLOW_ONBAL,
            fct.B2_STD_PROVISIONS_AMT_ONBAL,
            fct.B2_STD_EXPOSURE_VALUE_OFBAL,
            fct.B2_STD_CRM_TOT_OUTFLOW_OFBAL,
            fct.B2_STD_CRM_TOT_INFLOW_OFBAL,
            fct.B2_STD_PROVISIONS_AMT_OFBAL,
            fct.B2_STD_EFF_GTEE_AMT,
            fct.B2_STD_EFF_CDS_AMT,
            fct.B2_STD_OTH_FUN_CRED_PROT_AMT,
            fct.B2_STD_COLL_MTY_MISMATCH_AMT,
            fct.B2_STD_COLL_VOLATILITY_AMT,
            fct.B2_STD_COLL_FX_HAIRCUT_AMT,
            fct.B2_STD_FULLY_ADJ_EXP_CCF_100,
            fct.QCCP_FLAG,
            fct.B2_STD_RWA_PRE_SPRT_AMT,
            fct.B2_STD_RWA_PRE_SPRT_AMT_ONBAL,
            fct.B2_STD_RWA_PRE_SPRT_AMT_OFBAL,
            fct.B3_SME_DISCOUNT_FLAG,
            fct.B2_STD_RWA_AMT,
            fct.B2_STD_RWA_AMT_ONBAL,
            fct.B2_STD_RWA_AMT_OFBAL,
            fct.CRR2_501A_DISCOUNT_FLAG,
            fct.rule_desc,
            dim_corep_std.BASEL_EXPOSURE_TYPE,
            fct.CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_TYPE as CRD4_MEMO_BASEL_EXPOSURE_TYPE,
            fct.YEAR_MONTH,
            fct.REPORTING_REGULATOR_CODE,
            fct.RISK_TAKER_RRU RISK_TAKER_LL_RRU,
            fct.RISK_ON_RRU RISK_ON_LL_RRU,
            fct.INTERNAL_TRANSACTION_FLAG,
            fct.IG_EXCLUSION_FLAG,
            fct.DIVISION,
            fct.B3_SME_RP_FLAG,
            fct.REPORTING_TYPE_CODE,
            fct.RETAIL_POOL_SME_FLAG,
            fct.TRANSITIONAL_PORTFOLIO_FLAG,
            dim_corep_std.REG_EXPOSURE_TYPE,
            fct.STD_ACT_RISK_WEIGHT,
            fct.PROVIDER_B2_STD_ASSET_CLASS,
            fct.B2_STD_BASEL_EXPOSURE_TYPE,
            fct.COREP_STD_ASSET_CLASS_RK
        FROM {srtd_mart_db}.RWA_REPORTING_MART fct,    
            {frank_db}.DIM_ASSET_CLASS dim_corep_std,
            {rtd_ref_db}.DPM_PARAMETER d_parm_div
        WHERE fct.YEAR_MONTH = {v_year_month_rk}
           AND fct.REPORTING_REGULATOR_CODE IN ('UK-FSA', 'CBI')
           AND fct.RWA_APPROACH_CODE = 'STD'
           AND fct.ADJUST_FLAG = 'A'
           AND COALESCE(fct.REPORTING_TYPE_CODE, 'X') NOT IN ('OR', 'MR', 'SR')
           AND ((fct.CRD4_REP_STD_FLAG = 'Y') OR (fct.CRD4_REP_STDFLOW_FLAG = 'Y') OR
           (fct.ACLM_PRODUCT_TYPE = 'BASEL-OTC' AND fct.BASEL_DATA_FLAG = 'N') )     
           AND fct.DEFAULT_FUND_CONTRIB_INDICATOR = 'N'                      
           AND fct.DIVISION = d_parm_div.PARAMETER1_VALUE
           AND d_parm_div.param_code = 'DIVISION'
           AND d_parm_div.PARAMETER2_VALUE = 'Y'
           AND d_parm_div.valid_from_date <= '{p_reporting_date}'
           AND d_parm_div.valid_to_date > '{p_reporting_date}'
           AND dim_corep_std.ASSET_CLASS_RK = fct.COREP_STD_ASSET_CLASS_RK
    """
    input_df = spark.sql(query)
    return input_df

###################################################################################################################
#############    This function is called inside prepare_3_0_v1_c07_00 function.                         ###########
###################################################################################################################
def create_int_agg_df(spark, p_run_rk, p_return_rk, v_return_code, v_rep_catg):
    
    
    query = f'''
        SELECT 
            CAST((SUM((( COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0) + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5) 
                    + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0)) 
                    - ( (  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1) 
                    - (((COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW,0)) * -1)  + COALESCE(input_df.B2_STD_CRM_TOT_INFLOW,0)))
                    - ((COALESCE(input_df.B2_STD_PROVISIONS_AMT,0) ) * -1))) AS double ) AS
            C0010,
            CAST((SUM(((COALESCE(input_df.B2_STD_EXPOSURE_VALUE_ONBAL,0) - 
                    (CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET' 
                        THEN ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1)
                    ELSE 0
                    END ) ) -  (  ((COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW_ONBAL,0)) * -1) + COALESCE(input_df.B2_STD_CRM_TOT_INFLOW_ONBAL,0)))
                    - ((COALESCE(input_df.B2_STD_PROVISIONS_AMT_ONBAL,0)) * -1))) AS double) AS
            C0010_ONBAL,
            CAST((SUM((((  COALESCE(input_df.B2_STD_EXPOSURE_VALUE_OFBAL,0) + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                    + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))
                    - (CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
                        THEN ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1)
                        ELSE 0 END )) - (  ((COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW_OFBAL,0)) * -1) + COALESCE(input_df.B2_STD_CRM_TOT_INFLOW_OFBAL,0) ))
                    - ((COALESCE(input_df.B2_STD_PROVISIONS_AMT_OFBAL,0)) * -1) )) AS double) AS
            C0010_OFBAL,
            CAST((SUM((COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0) + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                    + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))
                    - ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0) ) * -1)
                    - ((COALESCE(input_df.B2_STD_PROVISIONS_AMT,0)) * -1))) AS double) AS
            C0010_MRTG,
            CAST((SUM((COALESCE(input_df.B2_STD_PROVISIONS_AMT,0) ) * -1)) AS double) AS C0030,
            CAST((SUM((COALESCE(input_df.B2_STD_PROVISIONS_AMT_ONBAL,0) ) * -1)) AS double) AS C0030_ONBAL,
            CAST((SUM((COALESCE(input_df.B2_STD_PROVISIONS_AMT_OFBAL,0) ) * -1)) AS double) AS C0030_OFBAL,
            CAST((SUM(  (  COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0)  + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0) )
                    - ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1)
                    - (  ((COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW,0)) * -1) + COALESCE(input_df.B2_STD_CRM_TOT_INFLOW,0)))) AS double) AS
            C0040,
            CAST((SUM(  (  COALESCE(input_df.B2_STD_EXPOSURE_VALUE_ONBAL,0) 
                    - (CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
                                THEN ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0)
                    + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1)
                    ELSE 0
                    END ) ) - (  (COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW_ONBAL,0) * -1)
                    + COALESCE(input_df.B2_STD_CRM_TOT_INFLOW_ONBAL,0)))) AS double) AS
            C0040_ONBAL,
            CAST((SUM(((COALESCE(input_df.B2_STD_EXPOSURE_VALUE_OFBAL,0) + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                    + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))
                - (CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
                        THEN  ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1)
                    ELSE 0
                    END ) ) - (  (COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW_OFBAL,0) * -1)
                    + COALESCE(input_df.B2_STD_CRM_TOT_INFLOW_OFBAL,0)))) AS double) AS
            C0040_OFBAL,            
            CAST((SUM((COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0) + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))
                - ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0) ) * -1))) AS double) AS
            C0040_MRTG,
            CAST((SUM(COALESCE(input_df.B2_STD_EFF_GTEE_AMT,0)) * -1) AS double) AS
            C0050,
            CAST((SUM(CASE
            WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
            THEN COALESCE(input_df.B2_STD_EFF_GTEE_AMT,0) * -1
            ELSE 0
            END)) AS double) AS C0050_ONBAL,
            CAST((SUM(CASE
            WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
            THEN COALESCE(input_df.B2_STD_EFF_GTEE_AMT,0) * -1
            ELSE 0
            END)) AS double) AS C0050_OFBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_EFF_CDS_AMT,0)) * -1) AS double) AS C0060,
            CAST((SUM(CASE
            WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
            THEN COALESCE(input_df.B2_STD_EFF_CDS_AMT,0) * -1
            ELSE 0
            END)) AS double) AS C0060_ONBAL,
            CAST((SUM(CASE
            WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
            THEN COALESCE(input_df.B2_STD_EFF_CDS_AMT,0) * -1
            ELSE 0
            END)) AS double) AS C0060_OFBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_OTH_FUN_CRED_PROT_AMT,0)) * -1) AS double) AS
            C0080,
            CAST((SUM(CASE
            WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
            THEN COALESCE(input_df.B2_STD_OTH_FUN_CRED_PROT_AMT,0) * -1
            ELSE 0
            END)) AS double) AS
            C0080_ONBAL,
            CAST((SUM(CASE
            WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
            THEN COALESCE(input_df.B2_STD_OTH_FUN_CRED_PROT_AMT,0) * -1
            ELSE 0
            END)) AS double) AS C0080_OFBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW,0)) * -1) AS double) AS C0090,
            CAST((SUM(COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW_ONBAL,0) * -1)) AS double) AS C0090_ONBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_CRM_TOT_OUTFLOW_OFBAL,0) * -1)) AS double) AS C0090_OFBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_CRM_TOT_INFLOW,0))) AS double) AS C0100,
            CAST((SUM(COALESCE(input_df.B2_STD_CRM_TOT_INFLOW_ONBAL,0))) AS double) AS C0100_ONBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_CRM_TOT_INFLOW_OFBAL,0))) AS double) AS C0100_OFBAL,
            CAST((SUM((COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0) + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8) + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))
                - ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1))) AS double) AS
            C0110,
            CAST((SUM(COALESCE(input_df.B2_STD_EXPOSURE_VALUE_ONBAL,0)
                - (CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
                    THEN ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0)
                    + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1)
                    ELSE 0
                    END))) AS double) AS
            C0110_ONBAL,
            CAST((SUM(  (  COALESCE(input_df.B2_STD_EXPOSURE_VALUE_OFBAL,0)
                + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
                + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8)
                + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))
                - ( CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
                        THEN ((  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0)
                        + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0) ) * -1)
                    ELSE 0
                    END)) ) AS double) AS
            C0110_OFBAL,
            CAST((SUM(  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0)
            + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1) AS double) AS
            C0130,
            CAST((SUM(CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
                    THEN (  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0)
                        + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1
                ELSE 0
                END)) AS double) AS
            C0130_ONBAL,
            CAST((SUM(CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
                    THEN (  COALESCE(input_df.B2_STD_EFF_FIN_COLL_AMT,0) + COALESCE(input_df.B2_STD_NETTED_COLL_GBP,0)) * -1
                ELSE 0
                END)) AS double) AS C0130_OFBAL,
            CAST((SUM(  COALESCE(input_df.B2_STD_COLL_MTY_MISMATCH_AMT,0)
                + COALESCE(input_df.B2_STD_COLL_VOLATILITY_AMT,0)
                + COALESCE(input_df.B2_STD_COLL_FX_HAIRCUT_AMT,0)) * -1) AS double) AS
            C0140,
            CAST((SUM(CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'ON BALANCE SHEET'
                    THEN (  COALESCE(input_df.B2_STD_COLL_MTY_MISMATCH_AMT,0)
                    + COALESCE(input_df.B2_STD_COLL_VOLATILITY_AMT,0)
                    + COALESCE(input_df.B2_STD_COLL_FX_HAIRCUT_AMT,0)) * -1
                ELSE 0 
                END)) AS double) AS
            C0140_ONBAL,
            CAST((SUM(CASE WHEN input_df.COREP_PRODUCT_CATEGORY = 'OFF BALANCE SHEET'
                    THEN (COALESCE(input_df.B2_STD_COLL_MTY_MISMATCH_AMT,0)
                    + COALESCE(input_df.B2_STD_COLL_VOLATILITY_AMT,0)
                    + COALESCE(input_df.B2_STD_COLL_FX_HAIRCUT_AMT,0)) * -1
                ELSE 0
                END)) AS double) AS
            C0140_OFBAL,
            CAST((SUM( COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0)
            + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
            + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8)
            + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))) AS double) AS
            C0150,
            CAST((SUM(COALESCE(input_df.B2_STD_EXPOSURE_VALUE_ONBAL,0))) AS double) AS C0150_ONBAL,
            CAST((SUM(  COALESCE(input_df.B2_STD_EXPOSURE_VALUE_OFBAL,0)
            + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0) * 0.5)
            + (COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0) * 0.8)
            + COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))) AS double) AS C0150_OFBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_0,0))) AS double) AS C0160,
            CAST((SUM(COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_20,0))) AS double) AS C0170,
            CAST((SUM(COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_50,0))) AS double) AS C0180,
            CAST((SUM(COALESCE(input_df.B2_STD_FULLY_ADJ_EXP_CCF_100,0))) AS double) AS C0190,
            CAST((SUM(COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0))) AS double) AS C0200,
            CAST((SUM(COALESCE(input_df.B2_STD_EXPOSURE_VALUE_ONBAL,0))) AS double) AS C0200_ONBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_EXPOSURE_VALUE_OFBAL,0))) AS double) AS C0200_OFBAL,
            CAST((SUM(CASE WHEN input_df.COREP_PRODUCT_CATEGORY IN('SECURITIES FINANCING', 'DERIVATIVES')
                    THEN COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0)
                ELSE 0
            END)) AS double) AS 
            C0210,
            CAST((SUM(CASE WHEN input_df.COREP_PRODUCT_CATEGORY IN('SECURITIES FINANCING', 'DERIVATIVES')
                        AND COALESCE(input_df.QCCP_FLAG,'X') <> 'Y'
                        THEN COALESCE(input_df.B2_STD_EXPOSURE_VALUE,0)
                ELSE 0
                END)) AS double) AS
            C0211,
            CAST((SUM(COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT,0))) AS double) AS C0215,
            CAST((SUM(COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT_ONBAL,0))) AS double) AS C0215_ONBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT_OFBAL,0))) AS double) AS C0215_OFBAL,
            CAST((SUM(CASE WHEN input_df.B3_SME_DISCOUNT_FLAG = 'Y' 
                    THEN (  COALESCE(input_df.B2_STD_RWA_AMT,0)
                            - COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT,0))
                ELSE 0
                END)) AS double) AS
            C0216,
            CAST((SUM(CASE WHEN input_df.B3_SME_DISCOUNT_FLAG = 'Y'
                    THEN (  COALESCE(input_df.B2_STD_RWA_AMT_ONBAL,0)
                            - COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT_ONBAL,0))
                ELSE 0
                END) ) AS double) AS
            C0216_ONBAL,
            CAST((SUM(CASE WHEN input_df.B3_SME_DISCOUNT_FLAG = 'Y'
                        THEN (  COALESCE(input_df.B2_STD_RWA_AMT_OFBAL,0)
                                - COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT_OFBAL,0))
                ELSE 0
                END)) AS double) AS
            C0216_OFBAL,
            CAST((SUM(CASE WHEN input_df.CRR2_501A_DISCOUNT_FLAG = 'Y'
                    THEN (  COALESCE(input_df.B2_STD_RWA_AMT,0)
                            - COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT,0))
                ELSE 0
                END) ) AS double) AS
            C0217,
            CAST((SUM(CASE WHEN input_df.CRR2_501A_DISCOUNT_FLAG = 'Y'
                        THEN ( COALESCE(input_df.B2_STD_RWA_AMT_ONBAL,0)
                                - COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT_ONBAL,0))
                ELSE 0
                END) ) AS double) AS
            C0217_ONBAL,
            CAST((SUM(CASE WHEN input_df.CRR2_501A_DISCOUNT_FLAG = 'Y'
                        THEN (  COALESCE(input_df.B2_STD_RWA_AMT_OFBAL,0)
                            - COALESCE(input_df.B2_STD_RWA_PRE_SPRT_AMT_OFBAL,0))
                ELSE 0
                END)) AS double) AS 
            C0217_OFBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_RWA_AMT,0))) AS double) AS C0220,
            CAST((SUM(COALESCE(input_df.B2_STD_RWA_AMT_ONBAL,0))) AS double) AS C0220_ONBAL,
            CAST((SUM(COALESCE(input_df.B2_STD_RWA_AMT_OFBAL,0))) AS double) AS C0220_OFBAL,
            CAST((SUM(CASE WHEN input_df.rule_desc IN ( 'RW set for Regional Government or Local Authority where valid rating exists for the counterparty (CQS Risk Weight Mapping RDS table)'
                                                ,'RW set for Admin and Non Commercial Undertaking where valid rating exists for the counterparty (CQS Risk Weight Mapping RDS table)' )
                    THEN COALESCE(input_df.B2_STD_RWA_AMT,0)
                ELSE 0
                END)) AS double) AS
            C0230,
            CAST((SUM(CASE WHEN input_df.rule_desc IN ( 'RW set for Regional Government or Local Authority where counterparty is unrated so Central government rating is used (CQS Risk Weight Mapping RDS table)'
                                                ,'RW set for Admin and Non Commercial Undertaking where counterparty is unrated so Central government rating is used (CQS Risk Weight Mapping RDS table)'
                                                ,'RW set for Unrated Institution where Original Effective Maturity > 3 months using Central government rating (CQS Risk Weight Mapping RDS table)' )
                    THEN COALESCE(input_df.B2_STD_RWA_AMT,0)
                ELSE 0
                END)) AS double) AS 
            C0240,
            CASE 
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to central governments or central banks'
                    THEN  'STD0002'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to regional governments or local authorities'
                    THEN 'STD0003'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to public sector entities'
                    THEN 'STD0004'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to multilateral development banks'
                    THEN 'STD0005'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to international organisations'
                    THEN 'STD0006'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to institutions'
                    THEN 'STD0007'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to corporates'
                    THEN 'STD0008'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Retail exposures'
                    THEN 'STD0009'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures Secured by Immovable Property'
                    THEN 'STD0010'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures in default'
                    THEN 'STD0011'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures associated with particularly high risk'
                    THEN 'STD0012'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures in the form of covered bonds'
                    THEN 'STD0013'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures to institutions and corporates with a  short-term credit assessment'
                    THEN 'STD0014'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Exposures in the form of units or shares in collective investment undertakings (''CIUs'')'
                    THEN 'STD0015'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Equity Claims'
                    THEN 'STD0016'
                WHEN input_df.BASEL_EXPOSURE_TYPE = 'Other items'
                    THEN 'STD0017'
            END  
            STD_SHEET_ID,
            CASE 
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE =  'Exposures to central governments or central banks'
                    THEN 'STD0002'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to regional governments or local authorities'
                    THEN 'STD0003'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to public sector entities'
                    THEN 'STD0004'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to multilateral development banks'
                    THEN 'STD0005'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to international organisations'
                    THEN 'STD0006'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to institutions'
                    THEN 'STD0007'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to corporates'
                    THEN 'STD0008'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Retail exposures'
                    THEN 'STD0009'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures Secured by Immovable Property'
                    THEN 'STD0010'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures in default'
                    THEN 'STD0011'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures associated with particularly high risk'
                    THEN 'STD0012'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures in the form of covered bonds'
                    THEN 'STD0013'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures to institutions and corporates with a  short-term credit assessment'
                    THEN 'STD0014'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Exposures in the form of units or shares in collective investment undertakings (''CIUs'')'
                    THEN 'STD0015'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Equity Claims'
                    THEN 'STD0016'
                WHEN input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE = 'Other items'
                    THEN 'STD0017'
            END  
            MEMO_SHEET_ID,
            input_df.YEAR_MONTH as YEAR_MONTH_RK,
            input_df.REPORTING_REGULATOR_CODE,
            input_df.RISK_TAKER_LL_RRU as RRU_RK,
            input_df.RISK_ON_LL_RRU as RISK_ON_RRU_RK,
            input_df.INTERNAL_TRANSACTION_FLAG,
            input_df.IG_EXCLUSION_FLAG,
            input_df.DIVISION,
            input_df.B3_SME_RP_FLAG,
            input_df.RETAIL_POOL_SME_FLAG,
            input_df.TRANSITIONAL_PORTFOLIO_FLAG,
            input_df.COREP_PRODUCT_CATEGORY,
            input_df.QCCP_FLAG,
            input_df.B3_SME_DISCOUNT_FLAG,
            input_df.CRR2_501A_DISCOUNT_FLAG,  
            input_df.REPORTING_TYPE_CODE,
            input_df.B2_STD_BASEL_EXPOSURE_TYPE,
            input_df.BASEL_EXPOSURE_TYPE  COREP_STD_BASEL_EXPOSURE_TYPE,
            input_df.REG_EXPOSURE_TYPE    COREP_STD_REG_EXPOSURE_TYPE,
            input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE,
            input_df.STD_ACT_RISK_WEIGHT,
            {p_run_rk}     RUN_RK,
            {p_return_rk}  RETURN_RK,
            '{v_return_code}'  RETURN_CODE,
            '{v_rep_catg}'   REPORTABLE_CATEGORY,
            NULL as FOR_ADJUST_FLAG
        FROM input_df_view input_df        
        GROUP BY
            input_df.YEAR_MONTH,
            input_df.REPORTING_REGULATOR_CODE,
            input_df.RISK_TAKER_LL_RRU,
            input_df.RISK_ON_LL_RRU,
            input_df.INTERNAL_TRANSACTION_FLAG,
            input_df.IG_EXCLUSION_FLAG,
            input_df.DIVISION,
            input_df.B3_SME_RP_FLAG,
            input_df.RETAIL_POOL_SME_FLAG,
            input_df.TRANSITIONAL_PORTFOLIO_FLAG,
            input_df.COREP_PRODUCT_CATEGORY,
            input_df.QCCP_FLAG,
            input_df.B3_SME_DISCOUNT_FLAG,
            input_df.CRR2_501A_DISCOUNT_FLAG,  
            input_df.REPORTING_TYPE_CODE,
            input_df.PROVIDER_B2_STD_ASSET_CLASS,
            input_df.COREP_STD_ASSET_CLASS_RK,
            input_df.CRD4_MEMO_BASEL_EXPOSURE_TYPE,
            input_df.STD_ACT_RISK_WEIGHT,
            input_df.B2_STD_BASEL_EXPOSURE_TYPE,
            input_df.BASEL_EXPOSURE_TYPE,
            input_df.REG_EXPOSURE_TYPE        
    '''

    df = spark.sql(query).persist(StorageLevel.MEMORY_AND_DISK_SER)
    return df

###################################################################################################################
#############    This function is called inside invoke_unpivot_logic function to create unpivot views   ###########
###################################################################################################################
def unpivot_int_agg_stage(stack,df,row_id,wherecondition = ''):
    
    dataframetouse = ''
    finaldf = None
    if(wherecondition == ''):
        #print("where condition is empty ")
        
        dataframetouse = df
    else:
        #print("where condition is not empty ",wherecondition)
        dataframetouse = df.filter(wherecondition)
        
        
    
    unpivotexp = stack
    unpivotdf = dataframetouse.select('YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','REPORTING_REGULATOR_CODE', \
                          'FOR_ADJUST_FLAG','RRU_RK','RISK_ON_RRU_RK', \
                          'INTERNAL_TRANSACTION_FLAG','IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY', \
                          'STD_SHEET_ID','STD_ACT_RISK_WEIGHT','COREP_STD_REG_EXPOSURE_TYPE','MEMO_SHEET_ID', expr(unpivotexp))\
                            .withColumn("ROW_ID",lit(row_id).cast(StringType())) \
                            .withColumnRenamed('STD_SHEET_ID' , 'SHEET_ID')

    if(row_id in ('R0010','R0015','R0020','R0030','R0035','R0040','R0283')):
        
        if(row_id == 'R0015'):
            unpivotdf = unpivotdf.withColumn('SHEET_ID',col('MEMO_SHEET_ID'))
        unpivotedf1 = unpivotdf.groupby( 'YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','COLUMN_ID','REPORTING_REGULATOR_CODE', \
                           'FOR_ADJUST_FLAG','SHEET_ID','ROW_ID', 'RRU_RK','RISK_ON_RRU_RK','INTERNAL_TRANSACTION_FLAG', \
                           'IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY').sum('VALUE') 
        #finaldf = unpivotedf1.withColumnRenamed('STD_SHEET_ID' , 'SHEET_ID').withColumnRenamed('sum(VALUE)' , 'VALUE')
        finaldf = unpivotedf1.withColumnRenamed('sum(VALUE)' , 'VALUE')
        
        
    elif(row_id in ('R0050','R0060')) :      
        
    
        unpivotedf1 = unpivotdf.withColumn('VALUE1',when(col('REPORTING_REGULATOR_CODE')=='DE-BBK',None).otherwise(col('VALUE')))
        
        unpivotedf2 = unpivotedf1.groupby( 'YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','COLUMN_ID','REPORTING_REGULATOR_CODE',
                                          'FOR_ADJUST_FLAG','SHEET_ID','ROW_ID', 'RRU_RK','RISK_ON_RRU_RK','INTERNAL_TRANSACTION_FLAG',
                                          'IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY').sum('VALUE1') 
        
        unpivotedf3 = unpivotedf2.drop(col('VALUE'))
        
        finaldf = unpivotedf3.withColumnRenamed('sum(VALUE1)' , 'VALUE')
        
        
    elif(row_id in ('R0070','R0080','R0090','R0100','R0110','R0120')):
        
        
        unpivotedf1 = unpivotdf.withColumn('COLUMN_ID_NEW',substring_index('COLUMN_ID','_',1))
        unpivotedf2 = unpivotedf1.drop('COLUMN_ID')
        unpivotedf3 = unpivotedf2.withColumnRenamed('COLUMN_ID_NEW','COLUMN_ID')
        unpivotedf4 = unpivotedf3.groupby( 'YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','COLUMN_ID','REPORTING_REGULATOR_CODE', \
                                          'FOR_ADJUST_FLAG','SHEET_ID','ROW_ID', 'RRU_RK','RISK_ON_RRU_RK','INTERNAL_TRANSACTION_FLAG', \
                                          'IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY').sum('VALUE') 
        
        finaldf = unpivotedf4.withColumnRenamed('sum(VALUE)' , 'VALUE')
        
    elif(row_id in ('R0140_R0280')):
        
        
        unpivotedf1 = unpivotdf.withColumn('ROW_ID1',when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.00, 'R0140')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.02, 'R0150')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.04, 'R0160')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.10, 'R0170')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.20, 'R0180')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.35, 'R0190')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.50, 'R0200')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.70, 'R0210')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==0.75, 'R0220')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==1.00, 'R0230')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==1.50, 'R0240')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==2.50, 'R0250')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==3.70, 'R0260')\
                                           .when(round(col('STD_ACT_RISK_WEIGHT'),3)==13.50, 'R0270')\
                                           .otherwise('R0280'))
        
        unpivotedf2 = unpivotedf1.withColumn('VALUE1',expr(' case when round(STD_ACT_RISK_WEIGHT,3)==0.70 and COLUMN_ID in ("C0010", "C0030", "C0040") then NULL else VALUE end'))
        
        unpivotedf3 = unpivotedf2.drop('VALUE','ROW_ID')
        
        unpivotedf4 = unpivotedf3.withColumnRenamed('VALUE1','VALUE').withColumnRenamed('ROW_ID1','ROW_ID')
        
        unpivotedf5 = unpivotedf4.groupby( 'YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','COLUMN_ID','REPORTING_REGULATOR_CODE',
                                          'FOR_ADJUST_FLAG','SHEET_ID','ROW_ID', 'RRU_RK','RISK_ON_RRU_RK','INTERNAL_TRANSACTION_FLAG',
                                          'IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY').sum('VALUE') 
        
        finaldf = unpivotedf5.withColumnRenamed('sum(VALUE)' , 'VALUE')
        
    elif(row_id in ('R0290_R0310')):
        
        unpivotedf1 = unpivotdf.withColumn('ROW_ID1',when(col('COREP_STD_REG_EXPOSURE_TYPE')== \
                                                          'Secured by mortgages on residential property','R0310').otherwise('R0290')) \
                                           .withColumn('COLUMN_ID_NEW',substring_index('COLUMN_ID','_',1)).withColumn('SHEET_ID',col('MEMO_SHEET_ID'))
        
        unpivotedf2 = unpivotedf1.groupby( 'YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','COLUMN_ID_NEW','REPORTING_REGULATOR_CODE', \
                                          'FOR_ADJUST_FLAG','SHEET_ID','ROW_ID1', 'RRU_RK','RISK_ON_RRU_RK','INTERNAL_TRANSACTION_FLAG', \
                                          'IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY').sum('VALUE') 
        
        unpivotedf3 = unpivotedf2.drop(col('VALUE')).drop(col('ROW_ID')).drop('COLUMN_ID')
        
        finaldf = unpivotedf3.withColumnRenamed('ROW_ID1' , 'ROW_ID').withColumnRenamed('sum(VALUE)' , 'VALUE').withColumnRenamed('COLUMN_ID_NEW' , 'COLUMN_ID')   
    
    elif(row_id in ('R0300_R0320')):
        
        
        unpivotedf1 = unpivotdf.withColumn('ROW_ID1',expr(' case when round(STD_ACT_RISK_WEIGHT,3) == 1.00 then "R0300" else "R0320" end ')).withColumn('SHEET_ID',col('MEMO_SHEET_ID'))
        
        unpivotedf2 = unpivotedf1.groupby( 'YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','COLUMN_ID','REPORTING_REGULATOR_CODE', \
                                          'FOR_ADJUST_FLAG','SHEET_ID','ROW_ID1', 'RRU_RK','RISK_ON_RRU_RK','INTERNAL_TRANSACTION_FLAG', \
                                          'IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY').sum('VALUE') 
        
        unpivotedf3 = unpivotedf2.drop(col('VALUE')).drop(col('ROW_ID'))
        
        finaldf = unpivotedf3.withColumnRenamed('ROW_ID1' , 'ROW_ID').withColumnRenamed('sum(VALUE)' , 'VALUE')                                          
        
        
    
    #Finally select only required columns : 
    
    finaldf = finaldf.withColumn("FOR_ADJUST_FLAG_NEW",lit(None).cast(StringType())).drop("FOR_ADJUST_FLAG")
    
    finaldf = finaldf.withColumnRenamed('FOR_ADJUST_FLAG_NEW','FOR_ADJUST_FLAG')
    
    
    finaldf = finaldf.select('YEAR_MONTH_RK','RETURN_RK','RETURN_CODE','REPORTING_REGULATOR_CODE', \
                      'FOR_ADJUST_FLAG','RRU_RK','RISK_ON_RRU_RK', \
                      'INTERNAL_TRANSACTION_FLAG','IG_EXCLUSION_FLAG','RUN_RK','REPORTABLE_CATEGORY', \
                      'SHEET_ID','ROW_ID','COLUMN_ID','VALUE')
    
        
    return finaldf

###################################################################################################################
#############    This function is called inside prepare_3_0_v1_c07_00 function to                       ###########
############# create unpivot view using unpivot_int_agg_stage                                           ###########
###################################################################################################################
def invoke_unpivot_logic(df):
    
    listofdf = []
    
    
    ###########################UNPIVOT LOGIC 1########################
    
    stack1 ="stack(24,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220,'C0230',C0230,'C0240',C0240) as (COLUMN_ID,VALUE) "

    row_id1='R0010'

    unpivot_stg_df1 = unpivot_int_agg_stage(stack1,df,row_id1)
    unpivot_stg_df_union1  = unpivot_stg_df1



    ###########################UNPIVOT LOGIC 2########################
    
    stack2 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id2='R0015'

    wherecondition2 = '''  COREP_STD_BASEL_EXPOSURE_TYPE == 'Exposures in default'
                     AND CRD4_MEMO_BASEL_EXPOSURE_TYPE IN ('Exposures associated with particularly high risk',
                                                           'Equity Claims') '''

    unpivot_stg_df2 = unpivot_int_agg_stage(stack2,df,row_id2,wherecondition2)
    unpivot_stg_df_union2 = unpivot_stg_df_union1.union(unpivot_stg_df2)



    ###########################UNPIVOT LOGIC 3########################
    
    stack3 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id3='R0020'

    wherecondition3 = '''   B3_SME_RP_FLAG = 'Y' OR RETAIL_POOL_SME_FLAG = 'Y' '''

    unpivot_stg_df3 = unpivot_int_agg_stage(stack3,df,row_id3,wherecondition3)
    unpivot_stg_df_union3 = unpivot_stg_df_union2.union(unpivot_stg_df3)



    ###########################UNPIVOT LOGIC 4########################
    
    stack4 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id4='R0030'

    wherecondition4 = '''  B3_SME_DISCOUNT_FLAG == 'Y'  '''

    unpivot_stg_df4 = unpivot_int_agg_stage(stack4,df,row_id4,wherecondition4)
    unpivot_stg_df_union4 = unpivot_stg_df_union3.union(unpivot_stg_df4)


    
    ############################UNPIVOT LOGIC 5########################

    stack5 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id5='R0035'

    wherecondition5 = ''' CRR2_501A_DISCOUNT_FLAG == 'Y'  '''

    unpivot_stg_df5 = unpivot_int_agg_stage(stack5,df,row_id5,wherecondition5)
    unpivot_stg_df_union5 = unpivot_stg_df_union4.union(unpivot_stg_df5)



    ###########################UNPIVOT LOGIC 6########################

    stack6 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id6='R0040'

    wherecondition6 = ''' COREP_STD_REG_EXPOSURE_TYPE IN ('Secured by mortgages on residential property') '''

    unpivot_stg_df6 = unpivot_int_agg_stage(stack6,df,row_id6,wherecondition6)
    unpivot_stg_df_union6 = unpivot_stg_df_union5.union(unpivot_stg_df6)



    ###########################UNPIVOT LOGIC 7########################

    stack7 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id7='R0050'

    wherecondition7 = '''   COALESCE(TRANSITIONAL_PORTFOLIO_FLAG,'X') <> 'Y' AND COALESCE(DIVISION,'X') <> 'CITIZENS' '''



    unpivot_stg_df7 = unpivot_int_agg_stage(stack7,df,row_id7,wherecondition7)

    unpivot_stg_df_union7 = unpivot_stg_df_union6.union(unpivot_stg_df7)

    
    ############################UNPIVOT LOGIC 8########################

    stack8 ="stack(22,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060,\
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,\
                        'C0140',C0140,'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,\
                        'C0190',C0190,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,\
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id8='R0060'

    wherecondition8 = '''   TRANSITIONAL_PORTFOLIO_FLAG == 'Y' OR DIVISION == 'CITIZENS' '''

    unpivot_stg_df8 = unpivot_int_agg_stage(stack8,df,row_id8,wherecondition8)
    unpivot_stg_df_union8 = unpivot_stg_df_union7.union(unpivot_stg_df8)



    ############################UNPIVOT LOGIC 9########################

    stack9 ="stack(17,'C0010_ONBAL',C0010_ONBAL,'C0030_ONBAL',C0030_ONBAL,'C0040_ONBAL',C0040_ONBAL,  \
                        'C0050_ONBAL',C0050_ONBAL,'C0060_ONBAL',C0060_ONBAL,'C0080_ONBAL',C0080_ONBAL, \
                        'C0090_ONBAL',C0090_ONBAL,'C0100_ONBAL',C0100_ONBAL,'C0110_ONBAL',C0110_ONBAL, \
                        'C0130_ONBAL',C0130_ONBAL,'C0140_ONBAL',C0140_ONBAL,'C0150_ONBAL',C0150_ONBAL, \
                        'C0200_ONBAL',C0200_ONBAL,'C0215_ONBAL',C0215_ONBAL,'C0216_ONBAL',C0216_ONBAL, \
                        'C0217_ONBAL',C0217_ONBAL,'C0220_ONBAL',C0220_ONBAL) as (COLUMN_ID,VALUE) "

    row_id9='R0070'


    unpivot_stg_df9 = unpivot_int_agg_stage(stack9,df,row_id9)
    unpivot_stg_df_union9 = unpivot_stg_df_union8.union(unpivot_stg_df9)



    ###########################UNPIVOT LOGIC 10########################

    stack10 ="stack(21,'C0010_OFBAL',C0010_OFBAL,'C0030_OFBAL',C0030_OFBAL,'C0040_OFBAL',C0040_OFBAL, \
                        'C0050_OFBAL',C0050_OFBAL,'C0060_OFBAL',C0060_OFBAL,'C0080_OFBAL',C0080_OFBAL, \
                        'C0090_OFBAL',C0090_OFBAL,'C0100_OFBAL',C0100_OFBAL,'C0110_OFBAL',C0110_OFBAL, \
                        'C0130_OFBAL',C0130_OFBAL,'C0140_OFBAL',C0140_OFBAL,'C0150_OFBAL',C0150_OFBAL, \
                        'C0160',C0160,'C0170',C0170,'C0180',C0180,'C0190',C0190,'C0200_OFBAL',C0200_OFBAL, \
                        'C0215_OFBAL',C0215_OFBAL,'C0216_OFBAL',C0216_OFBAL,'C0217_OFBAL',C0217_OFBAL, \
                        'C0220_OFBAL',C0220_OFBAL) as (COLUMN_ID,VALUE) "

    row_id10='R0080'            

    unpivot_stg_df10 = unpivot_int_agg_stage(stack10,df,row_id10)
    unpivot_stg_df_union10 = unpivot_stg_df_union9.union(unpivot_stg_df10)


    ###########################UNPIVOT LOGIC 11########################

    stack11 ="stack(7,'C0200',C0200,'C0210',C0210,'C0211',C0211,'C0215',C0215, \
            'C0216',C0216,'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id11='R0090'       

    wherecondition7 = '''  COREP_PRODUCT_CATEGORY == 'SECURITIES FINANCING' AND COALESCE(REPORTING_TYPE_CODE,'X') <> 'LS'  '''

    unpivot_stg_df11 = unpivot_int_agg_stage(stack11,df,row_id11,wherecondition7)
    unpivot_stg_df_union11 = unpivot_stg_df_union10.union(unpivot_stg_df11)



    ###########################UNPIVOT LOGIC 12########################

    stack12 ="stack(6,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,'C0217',C0217, \
                        'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id12='R0100'            

    wherecondition12 = '''  COREP_PRODUCT_CATEGORY == 'SECURITIES FINANCING'
                     AND COALESCE(REPORTING_TYPE_CODE,'X') <> 'LS'
                     AND QCCP_FLAG == 'Y'  '''

    unpivot_stg_df12 = unpivot_int_agg_stage(stack12,df,row_id12,wherecondition12)
    unpivot_stg_df_union12 = unpivot_stg_df_union11.union(unpivot_stg_df12)


    ###########################UNPIVOT LOGIC 13########################
    
    stack13 ="stack(7,'C0200',C0200,'C0210',C0210,'C0211',C0211,'C0215',C0215, \
                    'C0216',C0216,'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id13='R0110'            

    wherecondition13 = '''   COREP_PRODUCT_CATEGORY == 'DERIVATIVES' OR REPORTING_TYPE_CODE == 'LS'  '''

    unpivot_stg_df13 = unpivot_int_agg_stage(stack13,df,row_id13,wherecondition13)
    unpivot_stg_df_union13 = unpivot_stg_df_union12.union(unpivot_stg_df13)


    ###########################UNPIVOT LOGIC 14########################

    stack14 ="stack(6,'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216, \
                        'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id14='R0120'            

    wherecondition14 = '''   ( COREP_PRODUCT_CATEGORY == 'DERIVATIVES' OR REPORTING_TYPE_CODE == 'LS')
                     AND QCCP_FLAG == 'Y'  '''

    unpivot_stg_df14 = unpivot_int_agg_stage(stack14,df,row_id14,wherecondition14)
    unpivot_stg_df_union14 = unpivot_stg_df_union13.union(unpivot_stg_df14)



    ###########################UNPIVOT LOGIC 15########################

    stack15 ="stack(17,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0150',C0150,'C0160',C0160, \
                    'C0170',C0170,'C0180',C0180,'C0190',C0190,'C0200',C0200,'C0210',C0210, \
                    'C0211',C0211,'C0215',C0215,'C0216',C0216,'C0217',C0217,'C0220',C0220, \
                    'C0230',C0230,'C0240',C0240) as (COLUMN_ID,VALUE) "

    row_id15='R0140_R0280'            


    unpivot_stg_df15 = unpivot_int_agg_stage(stack15,df,row_id15)
    unpivot_stg_df_union15 = unpivot_stg_df_union14.union(unpivot_stg_df15)



    ###########################UNPIVOT LOGIC 16########################

    stack16 ="stack(24,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0050',C0050,'C0060',C0060, \
                        'C0080',C0080,'C0090',C0090,'C0100',C0100,'C0110',C0110,'C0130',C0130,'C0140',C0140, \
                        'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,'C0190',C0190,'C0200',C0200, \
                        'C0210',C0210,'C0215',C0215,'C0216',C0216,'C0217',C0217,'C0220',C0220,'C0230',C0230, \
                        'C0240',C0240) as (COLUMN_ID,VALUE) "

    row_id16='R0283'        

    wherecondition16 = ''' COREP_STD_REG_EXPOSURE_TYPE IN ('Collective Investment Undertakings') '''


    unpivot_stg_df16 = unpivot_int_agg_stage(stack16,df,row_id16,wherecondition16)
    unpivot_stg_df_union16 = unpivot_stg_df_union15.union(unpivot_stg_df16)


    
    ###########################UNPIVOT LOGIC 17########################

    stack17 ="stack(14,'C0010_MRTG',C0010_MRTG,'C0030',C0030,'C0040_MRTG',C0040_MRTG, \
                        'C0150',C0150,'C0160',C0160,'C0170',C0170,'C0180',C0180,'C0190',C0190, \
                        'C0200',C0200,'C0210',C0210,'C0215',C0215,'C0216',C0216,'C0217',C0217, \
                        'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id17='R0290_R0310'   


    wherecondition17 = ''' 
    (
    CRD4_MEMO_BASEL_EXPOSURE_TYPE IN ('Exposures to central governments or central banks',
                                                           'Exposures to regional governments or local authorities',
                                                           'Exposures to public sector entities',
                                                           'Exposures to institutions',
                                                           'Exposures to corporates',
                                                           'Retail exposures')
    
    AND B2_STD_BASEL_EXPOSURE_TYPE = 'Exposures Secured by Immovable Property'
                             AND (   (COALESCE(COREP_STD_REG_EXPOSURE_TYPE, 'X') <> 'Secured by mortgages on residential property')
                                  OR (COREP_STD_REG_EXPOSURE_TYPE = 'Secured by mortgages on residential property')))
    '''



    unpivot_stg_df17 = unpivot_int_agg_stage(stack17,df,row_id17,wherecondition17)
    unpivot_stg_df_union17 = unpivot_stg_df_union16.union(unpivot_stg_df17)



    ###########################UNPIVOT LOGIC 18########################

    stack18 ="stack(15,'C0010',C0010,'C0030',C0030,'C0040',C0040,'C0150',C0150, \
                        'C0160',C0160,'C0170',C0170,'C0180',C0180,'C0190',C0190, \
                        'C0200',C0200,'C0210',C0210,'C0211',C0211,'C0215',C0215, \
                        'C0216',C0216,'C0217',C0217,'C0220',C0220) as (COLUMN_ID,VALUE) "

    row_id18='R0300_R0320'   


    wherecondition18 = ''' 

     COREP_STD_BASEL_EXPOSURE_TYPE = 'Exposures in default' 
     AND CRD4_MEMO_BASEL_EXPOSURE_TYPE IN ('Exposures to central governments or central banks', 
                                           'Exposures to regional governments or local authorities', 
                                           'Exposures to public sector entities', 
                                           'Exposures to institutions', 
                                           'Exposures to corporates', 
                                           'Retail exposures') 
     AND ((ROUND(STD_ACT_RISK_WEIGHT,3) = 1.00)    
          OR (ROUND(STD_ACT_RISK_WEIGHT,3) = 1.50))   

    '''

    unpivot_stg_df18 = unpivot_int_agg_stage(stack18,df,row_id18,wherecondition18)
    unpivot_stg_df_union18 = unpivot_stg_df_union17.union(unpivot_stg_df18)


    unpivot_stg_df_union_final = unpivot_stg_df_union18
    
    return unpivot_stg_df_union_final

###################################################################################################################
#############    This function is called inside prepare_3_0_v1_c07_00 function                          ###########
#############   which is executing final agg. sql from int_agg_dp                                       ###########
###################################################################################################################
def final_aggregation(spark,v_rep_catg,v_year_month_rk,p_return_rk,v_return_code,
                     p_run_rk,p_reporting_date, rtd_mart_db, frank_db, v_for_Adjust_flag,rtd_ref_db,int_agg_dp):
    
    query1 = f'''
    
    SELECT iad.YEAR_MONTH_RK,
                      iad.RETURN_RK,
                      iad.RETURN_CODE,
                      iad.ROW_ID,
                      iad.COLUMN_ID,
                      iad.REPORTING_REGULATOR_CODE,
                      ig.RISK_TAKER_GROUP,
                      NULL  GROUP_TEXT,
                      iad.SHEET_ID,
                      iad.FOR_ADJUST_FLAG,
                      SUM (iad.VALUE)  VALUE,
                      iad.RUN_RK,
                      iad.REPORTABLE_CATEGORY
               FROM {int_agg_dp} iad,
                    {frank_db}.DIM_REP_INTRAGROUP_MAP ig
               WHERE iad.RRU_RK = ig.RISK_TAKER_RRU_RK
                 AND iad.RISK_ON_RRU_RK = ig.RISK_ON_RRU_RK
                 AND iad.YEAR_MONTH_RK = ig.YEAR_MONTH_RK
                 AND ig.RISK_TAKER_LL_RRU NOT LIKE '%DECON%'
                 AND iad.INTERNAL_TRANSACTION_FLAG = ig.INTERNAL_TRANSACTION_FLAG
                 AND ig.RISK_TAKER_GROUP IS NOT NULL
                 AND (   (    '{v_rep_catg}' = 'ALL'
                          AND ig.REPORTING_DESC = 'External')
                      OR (    ig.REPORTING_DESC = 'Inter Group'
                          AND COALESCE(iad.IG_EXCLUSION_FLAG,'N') <> 'Y'                         
                          AND iad.REPORTING_REGULATOR_CODE IN ('UK-FSA', 'CBI', 'NL-DNB')))  
               GROUP BY iad.YEAR_MONTH_RK,
                        iad.RETURN_RK,
                        iad.RETURN_CODE,
                        iad.ROW_ID,
                        iad.COLUMN_ID,
                        iad.REPORTING_REGULATOR_CODE,
                        ig.RISK_TAKER_GROUP,
                        iad.SHEET_ID,
                        iad.FOR_ADJUST_FLAG,
                        iad.RUN_RK,
                        iad.REPORTABLE_CATEGORY
    
    
    '''
    
    df1 = spark.sql(query1)
    
    
    query2 = f'''
    
    
    SELECT iad.YEAR_MONTH_RK,
                      iad.RETURN_RK,
                      iad.RETURN_CODE,
                      iad.ROW_ID,
                      iad.COLUMN_ID,
                      iad.REPORTING_REGULATOR_CODE,
                      ig.RISK_TAKER_GROUP,
                      NULL  GROUP_TEXT,
                      'STD0001'  SHEET_ID,
                      iad.FOR_ADJUST_FLAG,
                      SUM (iad.VALUE)  VALUE,
                      iad.RUN_RK,
                      iad.REPORTABLE_CATEGORY
               FROM {int_agg_dp} iad,
                    {frank_db}.DIM_REP_INTRAGROUP_MAP ig
               WHERE iad.RRU_RK = ig.RISK_TAKER_RRU_RK
                 AND iad.RISK_ON_RRU_RK = ig.RISK_ON_RRU_RK
                 AND iad.YEAR_MONTH_RK = ig.YEAR_MONTH_RK
                 AND ig.RISK_TAKER_LL_RRU NOT LIKE '%DECON%'
                 AND iad.INTERNAL_TRANSACTION_FLAG = ig.INTERNAL_TRANSACTION_FLAG
                 AND ig.RISK_TAKER_GROUP IS NOT NULL
                 AND (   (    '{v_rep_catg}' = 'ALL'
                          AND ig.REPORTING_DESC = 'External')
                      OR (    ig.REPORTING_DESC = 'Inter Group'
                          AND COALESCE(iad.IG_EXCLUSION_FLAG,'N') <> 'Y'
                          AND iad.REPORTING_REGULATOR_CODE IN ('UK-FSA', 'CBI', 'NL-DNB')))  
               GROUP BY iad.YEAR_MONTH_RK,
                        iad.RETURN_RK,
                        iad.RETURN_CODE,
                        iad.ROW_ID,
                        iad.COLUMN_ID,
                        iad.REPORTING_REGULATOR_CODE,
                        ig.RISK_TAKER_GROUP,
                        iad.FOR_ADJUST_FLAG,
                        iad.RUN_RK,
                        iad.REPORTABLE_CATEGORY
            
    
    '''
    
    df2 = spark.sql(query2)
    
    
    query3 = f'''
    
    
            SELECT  
                      {v_year_month_rk}  YEAR_MONTH_RK,
                      {p_return_rk}  RETURN_RK,
                      '{v_return_code}'  RETURN_CODE,
                      rcv.ROW_ID,
                      rcv.COLUMN_ID,
                      reg.MEMBER_CODE  REPORTING_REGULATOR_CODE,
                      risk_taker.MEMBER_CODE  RISK_TAKER_GROUP,
                      NULL  GROUP_TEXT,
                      sheet.member_code  SHEET_ID,
                      'A'  FOR_ADJUST_FLAG,
                      CASE
                        WHEN rcv.APPL_DATA_POINT_FLAG = 'Y'
                        THEN  0
                        ELSE NULL
                      END  VALUE,
                      {p_run_rk}  RUN_RK,
                      '{v_rep_catg}'  REPORTABLE_CATEGORY
            FROM {rtd_ref_db}.DIM_DPM_TEMPLATE_ROW_COL_VW rcv,
                    {rtd_ref_db}.DIM_DPM_MEMBER reg,
                    {rtd_ref_db}.DIM_DPM_MEMBER risk_taker,
                    {rtd_ref_db}.DIM_DPM_SHEET_VW sheet,
                    {rtd_ref_db}.DIM_DPM_RETURN dim_ret,
                    {rtd_ref_db}.DIM_DPM_TEMPLATE dim_tem,
                    {rtd_ref_db}.DPM_PARAMETER d_parm
               WHERE dim_ret.return_rk = {p_return_rk}
                 AND dim_ret.template_rk = dim_tem.template_rk
                 AND dim_tem.template_rk = rcv.template_rk
                 AND dim_ret.RETURN_CODE = '{v_return_code}'
                 AND dim_ret.VALID_FROM_DATE <= '{p_reporting_date}'
                 AND dim_ret.VALID_TO_DATE > '{p_reporting_date}'
                 AND rcv.REPORTING_REGULATOR_CODE IN ('UK-FSA')
                 AND rcv.reporting_regulator_code = reg.MEMBER_CODE
                 AND rcv.YEAR_MONTH_RK = {v_year_month_rk}
                 AND reg.VALID_FROM_DATE <= '{p_reporting_date}'
                 AND reg.VALID_TO_DATE > '{p_reporting_date}'
                 AND reg.DOMAIN_RK in (select domain_rk from {rtd_ref_db}.dim_dpm_domain
                                       where DOMAIN_CODE = 'REG_CODE'
                                         and VALID_FROM_DATE <= '{p_reporting_date}'
                                         and VALID_TO_DATE > '{p_reporting_date}')
                 AND risk_taker.VALID_FROM_DATE <= '{p_reporting_date}'
                 AND risk_taker.VALID_TO_DATE > '{p_reporting_date}'
                 AND risk_taker.DOMAIN_RK in (select domain_rk from {rtd_ref_db}.dim_dpm_domain
                                              where DOMAIN_CODE = 'REP_ENTITY'
                                                and VALID_FROM_DATE <= '{p_reporting_date}'
                                                and VALID_TO_DATE > '{p_reporting_date}')
                 AND sheet.YEAR_MONTH_RK = {v_year_month_rk}
                 AND sheet.reporting_regulator_code = reg.MEMBER_CODE
                 AND sheet.DOMAIN_RK in (select domain_rk from {rtd_ref_db}.dim_dpm_domain
                                         where DOMAIN_CODE = 'EC_COREP_STD'
                                           and VALID_FROM_DATE <= '{p_reporting_date}'
                                           and VALID_TO_DATE > '{p_reporting_date}')
                 AND reg.MEMBER_CODE = d_parm.PARAMETER1_VALUE
                 AND risk_taker.MEMBER_CODE = d_parm.PARAMETER2_VALUE
                 AND d_parm.PARAM_CODE = 'REGULATOR_REP_ENTITY_MAP'
                 AND d_parm.VALID_FROM_DATE <= '{p_reporting_date}'
                 AND d_parm.VALID_TO_DATE > '{p_reporting_date}'
    
    
    '''
    
    df3 = spark.sql(query3)
    
    finaldf = df1.union(df2).union(df3)
    
    finaldf = finaldf.withColumn("GROUP_TEXT",lit(None).cast(StringType()))\
                    .withColumn("TEXT_VALUE",lit(None).cast(StringType()))\
                    .withColumn("DIVISION",lit(None).cast(StringType()))\
                    .withColumn("APPL_DATA_POINT_FLAG" ,lit(None).cast(StringType()))\
                    .withColumn("EXTRACT_ROW_ID" ,lit(None).cast(StringType()))
    

    return finaldf 

###################################################################################################################
#############    This function is called inside pr_3_0_v1_c07_00 function                               ###########
############# which is creating int_agg_dp from int_agg and unpivot and                                 ###########
############# the from int_agg_dp it is creating final_df                                               ###########
###################################################################################################################
def prepare_3_0_v1_c07_00(spark, rtd_mart_db, srtd_mart_db, frank_db, p_run_rk,p_return_rk,v_return_code,p_reporting_date,v_year_month_rk, v_for_Adjust_flag,input_df_view,rtd_ref_db):
    
    #input_df_view = get_input_data_for_int_agg_df(spark, rtd_mart_db, srtd_mart_db, frank_db,v_year_month_rk, p_reporting_date)

    input_df_view = input_df_view.persist(StorageLevel.MEMORY_AND_DISK_SER)
    input_df_view.createOrReplaceTempView('input_df_view')

    listoftries = [1,2]    

    for i in listoftries:
        
        #print("i value is ",i)
        
        if(i == 1):
            v_rep_catg = 'INTRA'
            df = create_int_agg_df(spark, p_run_rk,p_return_rk,v_return_code,v_rep_catg)
            listofdf_intra = invoke_unpivot_logic(df)
            listofdf_intra.createOrReplaceTempView('INT_AGG_DP_INTRA')
            int_agg_dp = 'INT_AGG_DP_INTRA'
            final_df1 = final_aggregation(spark,v_rep_catg,
                                          v_year_month_rk,p_return_rk,v_return_code,
                                          p_run_rk,p_reporting_date, rtd_mart_db, frank_db, v_for_Adjust_flag,rtd_ref_db,int_agg_dp)

        elif(i == 2):            
            v_rep_catg = 'ALL'
            df = create_int_agg_df(spark, p_run_rk,p_return_rk,v_return_code,v_rep_catg)
            listofdf_all = invoke_unpivot_logic(df)
            listofdf_all.createOrReplaceTempView('INT_AGG_DP_ALL')
            int_agg_dp = 'INT_AGG_DP_ALL'
            final_df2 = final_aggregation(spark,v_rep_catg,
                                          v_year_month_rk,p_return_rk,v_return_code,
                                          p_run_rk,p_reporting_date, rtd_mart_db, frank_db, v_for_Adjust_flag,rtd_ref_db,int_agg_dp)
          
    final_result_df = final_df1.union(final_df2)
    
    return final_result_df

###################################################################################################################
############  This function is the main function for spark-submit.                                        #########
###################################################################################################################
def pr_3_0_v1_c07_00():
    
    # Initialisation Part
    spark = get_spark_session()
    config = read_config(spark)
    v_return_code = 'C 07.00'
    # p_reporting_regulator_code = 'UK-FSA'
    return_code = v_return_code


    # Parameters from config file
    reporting_date = config["Input_parameters"]["reporting_date"]
    p_reporting_date = datetime.strptime(reporting_date, '%Y-%m-%dT%H::%M::%S.%f').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    v_for_Adjust_flag = config["Input_parameters"]["for_Adjust_flag"]
    rtd_ref_db = config["Database_details"]["rtd_ref_db"]
    rtd_mart_db = config["Database_details"]["rtd_mart_db"]
    srtd_mart_db = config["Database_details"]["srtd_mart_db"]
    frank_db = config["Database_details"]["frank_db"]
    sfrank_db = config["Database_details"]["sfrank_db"]
    p_run_type = config["Input_parameters"]["run_type"]
    p_batch_type = config["Input_parameters"]["batch_type"]
    v_stg_table_name = config["Input_parameters"]["stg_table_name"]
    p_run_rk = config["Input_parameters"]["run_rk"]
    
    # Calling functions to get value for return_rk based on input parameters
    p_return_rk = get_return_rk(v_return_code,p_reporting_date,rtd_ref_db)
    v_year_month_rk = datetime.strptime(reporting_date,'%Y-%m-%dT%H::%M::%S.%f').strftime('%Y%m')
    v_return_rk = p_return_rk 

    input_df_view = get_input_data_for_int_agg_df(spark, rtd_mart_db, srtd_mart_db, frank_db,v_year_month_rk, p_reporting_date,rtd_ref_db)
    
    final_df = prepare_3_0_v1_c07_00(spark, rtd_mart_db, srtd_mart_db, frank_db, p_run_rk,p_return_rk,v_return_code, p_reporting_date,v_year_month_rk, v_for_Adjust_flag,input_df_view,rtd_ref_db)
    final_df = final_df.select('return_rk','row_id','column_id','reporting_regulator_code','risk_taker_group','for_adjust_flag','value','run_rk','sheet_id','reportable_category','group_text','text_value','division','appl_data_point_flag','extract_row_id','year_month_rk','return_code')

    print("**************************WRITING INTO STAGING TABLE*************************")
    #final_df.write.format("parquet").partitionBy("return_code").mode("append").saveAsTable(srtd_mart_db + '.' + v_stg_table_name)
    final_df.write.format("parquet").mode("overwrite").insertInto(srtd_mart_db + '.' + v_stg_table_name,overwrite=True)
       
    # Load Function
    for adjust_flag in ('A', 'U'):
        load_fct_dpm_return(p_reporting_date,v_return_code,p_run_rk,adjust_flag,v_stg_table_name,p_return_rk,v_year_month_rk,rtd_ref_db,srtd_mart_db,sfrank_db,spark)
        
pr_3_0_v1_c07_00()