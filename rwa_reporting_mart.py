from pyspark import StorageLevel
import time
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import Row, DoubleType, StringType
from pyspark.sql.functions import col, lit, sum, broadcast, expr
from pyspark.sql.types import *
from modules.common_function import get_spark_session
from modules.common_function import read_config

import datetime

start = time.time()

def data_cleansing(row):
    current_row = list(row);

    # b2_irb_ac_reg_exposure_sub_type
    if (current_row[3] is None):
        current_row[3] = "";

    # approved_app_code
    if (current_row[6] is None):
        current_row[6] = "";

    # b2_std_ac_basel_expo_typ
    if (current_row[8] is None):
        current_row[8] = "";

    #reporting_type_code
    if (current_row[9] is None):
        current_row[9] = "";

    # b2_app_ead_post_crm_amt
    if (current_row[14] is None):
        current_row[14] = 0.0;

    # ifrs9_tot_regu_eclamt_gbp
    if (current_row[16] is None):
        current_row[16] = 0.0;

    # provision_amt_gbp
    if (current_row[17] is None):
        current_row[17] = 0.0;

    # b3_cva_provision_amt
    if (current_row[19] is None):
        current_row[19] = 0.0;

    # b2_std_rwa_post_crm_amt
    if (current_row[20] is None):
        current_row[20] = 0.0;

    # b2_app_rwa_post_crm_amt
    if (current_row[21] is None):
        current_row[21] = 0.0;

    # retail_off_bal_sheet_flag
    if (current_row[22] is None):
        current_row[22] = 'X';

    # b2_std_drawn_rwa
    if (current_row[23] is None):
        current_row[23] = 0.0;

    # b2_std_undrawn_rwa
    if (current_row[24] is None):
        current_row[24] = 0.0;

    # aclm_product_type
    if (current_row[25] is None):
        current_row[25] = "";

    # b2_irb_undrawn_rwa
    if (current_row[26] is None):
        current_row[26] = 0.0;

    # b2_app_adjusted_pd_ratio
    if (current_row[27] is None):
        current_row[27] = 0.0;

    # b2_app_adjusted_lgd_ratio
    if (current_row[28] is None):
        current_row[28] = 0.0;

    # b2_irb_drawn_ead
    if (current_row[47] is None):
        current_row[47] = 0.0;

    # b2_irb_undrawn_ead
    if (current_row[48] is None):
        current_row[48] = 0.0;

    # bank_base_role_code
    if (current_row[49] is None):
        current_row[49] = "";

    # off_bal_led_exp_gbp_post_sec
    if (current_row[50] is None):
        current_row[50] = 0.0;

    # undrawn_commitment_amt_pst_sec
    if (current_row[51] is None):
        current_row[51] = 0.0;

    # off_balance_exposure
    if (current_row[52] is None):
        current_row[52] = 0.0;

    # undrawn_commitment_amt
    if (current_row[53] is None):
        current_row[53] = 0.0;

    # b2_irb_effective_maturity_yrs
    if (current_row[54] is None):
        current_row[54] = 0.0;

    # b2_irb_drawn_expected_loss
    if (current_row[57] is None):
        current_row[57] = 0.0;

    # b2_app_expected_loss_amt
    if (current_row[58] is None):
        current_row[58] = 0.0;

    # unsettled_price_difference_amt
    if (current_row[83] is None):
        current_row[83] = 0.0; 

    # COREP_PRODUCT_CATEGORY
    if (current_row[18] is None):
        current_row[18] = "UNKNOWN"; 

    return current_row;
# end of data_cleansing()

def data_cleansing_for_sub_exposures(row):
    current_row = list(row);

    # b2_std_eff_gtee_amt
    if (current_row[3] is None):
        current_row[3] = 0.0;

    # b2_std_eff_cds_amt
    if (current_row[4] is None):
        current_row[4] = 0.0;

    # b2_std_gtee_cds_rwa
    if (current_row[6] is None):
        current_row[6] = "";

    # corep_crm_category
    if (current_row[8] is None):
        current_row[8] = "";

    # sec_discntd_b2_irb_garntee_amt
    if (current_row[9] is None):
        current_row[9] = 0.0;

    # double_default_flag
    if (current_row[10] is None):
        current_row[10] = "N";

    # b2_irb_gtee_amt
    if (current_row[11] is None):
        current_row[11] = 0.0;

    # b2_irb_coll_amt
    if (current_row[13] is None):
        current_row[13] = 0.0;

    # b2_std_fx_haircut
    if (current_row[15] is None):
        current_row[15] = 0.0;

    # b2_std_elig_coll_amt
    if (current_row[16] is None):
        current_row[16] = 0.0;

    return current_row;
# end of data_cleansing_for_sub_exposures()

###############################################################
#########----------Exposures Functions-----------########
###############################################################

def get_prev_qtr_date(reporting_date, rtd_ref_db):
    query_prev_date = spark.sql(f"select add_months(to_date('{reporting_date}', 'dd-MM-yyyy'), -3)").collect()[0][0]
    query_prev_date_array = str(query_prev_date).split("-")
    query_prev_date = query_prev_date_array[0] + "-" + query_prev_date_array[1] + "-" + query_prev_date_array[2]
    
    query = f'''SELECT quarter_end_date
            FROM {rtd_ref_db}.dim_year_month
            WHERE month_end_date = '{query_prev_date}'
            '''
    fin_val = spark.sql(query).collect()[0][0]
    fin_val_array = str(fin_val).split("-")
    result = int(fin_val_array[0] + fin_val_array[1])

    return result
# end get_prev_qtr_date()


def derived_prev_quarter_atributes(data): 
    derived_atributes_list = [];
    
    if (data is None or len(data) == 0 or data == 'No Data Found'):
        reporting_regulator_code_qtr = "";
        external_counterparty_id_qtr = "Unknown";
        rwa_approach_code_qtr = "";
        memo_b2_std_basel_exposure_type_qtr = "";
        b2_app_adjusted_pd_ratio_qtr = 0;
    else: 
        if (data[0] is None):
            reporting_regulator_code_qtr = "";
        else:
            reporting_regulator_code_qtr = data[0];
            
        if (data[1] is None):
            external_counterparty_id_qtr = ""
        else:
            external_counterparty_id_qtr = data[1];
            
        if (data[2] is None):
            rwa_approach_code_qtr = "";
        else:
            rwa_approach_code_qtr = data[2];
            
        if (data[3] is None):
            memo_b2_std_basel_exposure_type_qtr = ""
        else:
            memo_b2_std_basel_exposure_type_qtr = data[3];
            
        if (data[4] is None):
            b2_app_adjusted_pd_ratio_qtr = 0;
        else:
            b2_app_adjusted_pd_ratio_qtr = data[4];

    std_pre_dflt = calculate_std_pre_dflt(rwa_approach_code_qtr, memo_b2_std_basel_exposure_type_qtr);

    airb_pre_dflt = calculate_airb_pre_dflt(rwa_approach_code_qtr, b2_app_adjusted_pd_ratio_qtr);

    derived_atributes_list = derived_atributes_list + [std_pre_dflt, airb_pre_dflt]

    return derived_atributes_list
# end derived_prev_quarter_atributes()


def get_prev_qtr_df(crd4_prev_qtr_date, srtd_mart_db):
    try:

        prev_qrtr_df = spark.sql(f'''select * from {srtd_mart_db}.rwa_reporting_mart where year_month={crd4_prev_qtr_date} 
                            and flow_type = 'EXPOSURE' and ADJUST_FLAG='A' and ((RWA_APPROACH_CODE = 'STD'
                            and MEMO_B2_STD_BASEL_EXPOSURE_TYPE != 'Exposures in default')
                            or (RWA_APPROACH_CODE = 'AIRB' and coalesce(B2_APP_ADJUSTED_PD_RATIO, 0.2501) != 1)) 
                            and EXTERNAL_COUNTERPARTY_ID is not NULL''').distinct();
        prev_qrtr_df = prev_qrtr_df.select("deal_id", "reporting_regulator_code", "external_counterparty_id",
                                    "rwa_approach_code", "memo_b2_std_basel_exposure_type",
                                    "b2_app_adjusted_pd_ratio", "year_month")
        prev_qrtr_df = prev_qrtr_df.fillna({"deal_id": "", "reporting_regulator_code": "","external_counterparty_id": "UnKnown", 
                                        "rwa_approach_code": "","memo_b2_std_basel_exposure_type": "", "b2_app_adjusted_pd_ratio": 0})
    
        if (prev_qrtr_df.count() > 0):
            prev_qrtr_pairs = prev_qrtr_df.rdd.map(lambda a: (a[0],(a[1], a[2], a[3], a[4], a[5]))).toDF();
            prev_qarter_value_pairs = prev_qrtr_pairs.select(prev_qrtr_pairs._1.alias("Key"),
                                                        prev_qrtr_pairs._2.alias("Prev_Quarter_Values"));
            prev_qarter_value_pairs = prev_qarter_value_pairs.persist(StorageLevel.MEMORY_AND_DISK_SER);
        else:
            data = [(99999, ['No Data Found'])]
            columns = StructType([StructField('Key', IntegerType(), True),
                            StructField('Prev_Quarter_Values', ArrayType(StringType(), True))])
            prev_qarter_value_pairs = spark.createDataFrame(data=data, schema=columns)

        return prev_qarter_value_pairs;
    except Exception as e:
        print("\n\n\nException occurred in getting previous quarter data : ", e);
        print("\n\n");
        data = [(99999, ['No Data Found'])];
        columns = StructType([StructField('Key', IntegerType(), True),
                            StructField('Prev_Quarter_Values', ArrayType(StringType(), True))]);
        prev_qarter_value_pairs = spark.createDataFrame(data=data, schema=columns);
        return prev_qarter_value_pairs;
# end get_prev_qtr_df()


def calculate_rwa_approach_code(reporting_reg_code, model_code, b2_irb_ac_basel_exposure_type,
                                b2_irb_ac_reg_exposure_sub_type, dim_div_sub2, approved_app_code):
    rwa_approach_code = "";

    if (reporting_reg_code == "UK-FSA"):
        if (dim_div_sub2 in ('CITIZENS2001', 'CITIZENS2002', 'CITIZENS2003', 'CITIZENS2001_NC',
                            'CITIZENS2002_NC', 'CITIZENS2003_NC')):
            rwa_approach_code = 'STD';
        else:
            rwa_approach_code = approved_app_code;
    elif (reporting_reg_code == "NL-DNB"):
        if ((model_code == "LC") or (b2_irb_ac_reg_exposure_sub_type in ('Income Producing Real Estate',
                                                                        'Project Finance') and (b2_irb_ac_basel_exposure_type == 'Corporate'))):
            rwa_approach_code = 'AIRB';
        else:
            rwa_approach_code = 'STD';
    elif (reporting_reg_code == "DE-BBK"):
        rwa_approach_code = approved_app_code
    else:
        rwa_approach_code = approved_app_code

    return rwa_approach_code;
# end of calculate_rwa_approach_code()


def calculate_ig_exclusion_flag(reporting_regular_code, internal_transaction_flag, override_value, v_icb_eff_from_year_month_rk, year_month, 
                                risk_on_gs_code, risk_taker_group_structure, risk_on_group_structure, risk_taker_gs_code, 
                                dim_ctrpty_risk_entity_class, risk_taker_ll_rru,v_core_ig_eff_year_month_rk):
    ig_exclusion_flag = "";
    temp_flag = "N";

    if(year_month is None or len(str(year_month)) == 0):
        year_month = 0;
    else:
        year_month = int(year_month)

    if (v_icb_eff_from_year_month_rk is None or len(str(v_icb_eff_from_year_month_rk)) == 0):
        v_icb_eff_from_year_month_rk = 0;
    else:
        if (v_icb_eff_from_year_month_rk == 'N'):
            v_icb_eff_from_year_month_rk = 0;
        v_icb_eff_from_year_month_rk = int(float(v_icb_eff_from_year_month_rk)) 

    if (v_core_ig_eff_year_month_rk is None or len(str(v_core_ig_eff_year_month_rk)) == 0):
        v_core_ig_eff_year_month_rk = 0;
    else:
        if (v_core_ig_eff_year_month_rk == 'CORE'):
            v_core_ig_eff_year_month_rk = 0;
        v_core_ig_eff_year_month_rk = int(float(v_core_ig_eff_year_month_rk))


    if (year_month < v_icb_eff_from_year_month_rk):      
        if ((risk_taker_group_structure is not None) and (risk_on_group_structure is not None)):         
            if ((risk_taker_group_structure.upper() == 'UKIG') and (risk_on_group_structure.upper() == 'WIG')) or ((risk_taker_group_structure.upper() == 'UKIG') and (risk_on_group_structure.upper() == 'NIG')) or ((risk_taker_group_structure.upper() == 'WIG') and (risk_on_group_structure.upper() == 'NIG')):    
                temp_flag = "N";
            else:
                temp_flag = "Y";
        else:
            temp_flag = "N";
    else:
        if ((risk_on_gs_code is not None) and (risk_on_gs_code.upper() == 'CONNECTEDC_XREF')):
            temp_flag = "N";
        elif ((risk_taker_gs_code is not None) and (risk_on_gs_code is not None) and (risk_taker_ll_rru is not None)):
            if (((risk_taker_gs_code.upper() == 'CORE') and (risk_on_gs_code.upper() == 'NONCORE')) or ((risk_taker_gs_code.upper() == 'CORE') and (risk_on_gs_code.upper() == 'CONNECTEDC')) or ((risk_taker_gs_code.upper() == 'NONCORE') and (risk_on_gs_code.upper() == 'CONNECTEDC')) or ((risk_taker_gs_code.upper() == 'CONNECTEDC') and (risk_on_gs_code.upper() == 'CONNECTEDC')) or (risk_taker_ll_rru.upper() == 'ULSTERAI') or ((year_month >= v_core_ig_eff_year_month_rk) and (risk_taker_gs_code.upper() == 'CORE') and (risk_on_gs_code.upper() == 'CORE'))):
                temp_flag = "N";
            else:
                temp_flag = "Y";
        else:
            temp_flag = "N";


    if ((reporting_regular_code is not None) and (reporting_regular_code in ('CBI', 'NL-DNB'))):
        ig_exclusion_flag = "N";
    else:
        if ((internal_transaction_flag is not None) and (internal_transaction_flag.upper() == "Y")):
            if(((override_value is not None) and (override_value.upper() == "Y")) or ((dim_ctrpty_risk_entity_class is not None) and (dim_ctrpty_risk_entity_class not in ('Bank', 'Corporate'))) or (temp_flag == "Y")):
                ig_exclusion_flag = "Y";
            else:
                ig_exclusion_flag = "N";

    return ig_exclusion_flag;

def calculate_corep_ac_bsl_expo_typ(b2_std_ac_basel_expo_typ, reporting_type_code):
    corep_ac_bsl_expo_typ = "";

    if ((reporting_type_code == 'NC') and (b2_std_ac_basel_expo_typ not in ('Equity Claims',
                                            'Exposures to central governments or central banks'))):
        corep_ac_bsl_expo_typ = "Other items";
    else:
        corep_ac_bsl_expo_typ = b2_std_ac_basel_expo_typ;

    return corep_ac_bsl_expo_typ;
# end of calculate_corep_ac_bsl_expo_typ()


def calculate_std_rsk_wght_ratio(rwa_approach_code, b2_std_rsk_wght_ratio):
    std_rsk_wght_ratio = 0.0;

    if (rwa_approach_code == 'AIRB'):
        std_rsk_wght_ratio = 0.0;
    else:
        std_rsk_wght_ratio = b2_std_rsk_wght_ratio;

    return std_rsk_wght_ratio;
# end of calculate_std_rsk_wght_ratio()


def calculate_crd4_rep_std_flag(rwa_approach_code, corep_std_ac_basl_expo_typ, basel_data_flag):
    crd4_rep_std_flag = "";

    if ((rwa_approach_code == 'STD') and (corep_std_ac_basl_expo_typ != 'Items representing securitisation positions')
        and (basel_data_flag == 'Y')):
        crd4_rep_std_flag = 'Y'
    else:
        crd4_rep_std_flag = 'N';

    return crd4_rep_std_flag;
# end of calculate_crd4_rep_std_flag()


def calculate_b2_std_crm_tot_outflow_rwa_indv(b2_std_eff_gtee_amt, b2_std_eff_cds_amt, type_value,
                                            mitigant_type, b2_std_gtee_cds_rwa):
    b2_std_crm_tot_outflow_rwa_indv = 0.0;

    if (((b2_std_eff_gtee_amt > 0) or (b2_std_eff_cds_amt > 0)) and
            (type_value == "WG") and (mitigant_type in ('GU', 'CD'))):
        b2_std_crm_tot_outflow_rwa_indv = b2_std_gtee_cds_rwa;
    else:
        b2_std_crm_tot_outflow_rwa_indv = 0.0;

    return b2_std_crm_tot_outflow_rwa_indv;
# end of calculate_b2_std_crm_tot_outflow_rwa_indv()


def calculate_b2_std_crm_tot_outflow_indv(b2_std_eff_gtee_amt, b2_std_eff_cds_amt, type_value,
                                        mitigant_type):
    b2_std_crm_tot_outflow_indv = 0.0;

    if (((b2_std_eff_gtee_amt > 0) or (b2_std_eff_cds_amt > 0)) and
            (type_value == "WG") and (mitigant_type in ('GU', 'CD'))):
        b2_std_crm_tot_outflow_indv = b2_std_eff_gtee_amt + b2_std_eff_cds_amt;
    else:
        b2_std_crm_tot_outflow_indv = 0.0;

    return b2_std_crm_tot_outflow_indv;
# end of calculate_b2_std_crm_tot_outflow_indv()


def calculate_b2_std_rwa_amt(b2_app_rwa_post_crm_amt, b2_std_crm_tot_outflow_rwa):
    if(b2_std_crm_tot_outflow_rwa is None):
        b2_std_crm_tot_outflow_rwa = 0.0;
        
    if(b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;

    b2_std_rwa_amt = (b2_app_rwa_post_crm_amt - b2_std_crm_tot_outflow_rwa);

    return b2_std_rwa_amt;
# end of calculate_b2_std_rwa_amt()


def calculate_rep_provision_amt_gbp(extrnl_cntrparty_id, deal_id, ifrs9_tot_regu_eclamt_gbp, 
                                    provision_amt_gbp):

    if ((extrnl_cntrparty_id == "Z6VROIT") or ("AR2956" in deal_id)):
        rep_provision_amt_gbp = (ifrs9_tot_regu_eclamt_gbp + provision_amt_gbp);
    else:
        rep_provision_amt_gbp = ifrs9_tot_regu_eclamt_gbp;

    return rep_provision_amt_gbp;
# end of calculate_rep_provision_amt_gbp()


def calculate_b2_std_provisions_amt_onbal(corep_product_category,b3_cva_provision_amt,rep_provision_amt_gbp):

    if (rep_provision_amt_gbp is None):
        rep_provision_amt_gbp = 0.0;

    if ((corep_product_category is not None) and
            (corep_product_category in ('ON BALANCE SHEET', 'N/A', 'UNKNOWN'))):
        b2_std_provisions_amt_onbal = (rep_provision_amt_gbp + b3_cva_provision_amt);
    else:
        b2_std_provisions_amt_onbal = 0.0;

    return b2_std_provisions_amt_onbal;
# end of calculate_b2_std_provisions_amt_onbal()


def calculate_rwa_post_crm_amt(citizens_override_flag, b2_std_rwa_post_crm_amt, b2_app_rwa_post_crm_amt):

    if ((citizens_override_flag is not None) and (citizens_override_flag == "Y")):
        rwa_post_crm_amt = b2_std_rwa_post_crm_amt;
    else:
        rwa_post_crm_amt = b2_app_rwa_post_crm_amt;

    return rwa_post_crm_amt;
# end of calculate_rwa_post_crm_amt()


def calculate_b2_std_rwa_amt_ofbal(corep_product_category, rwa_post_crm_amt, b2_std_gtee_cds_rwa,
                                b2_std_gtee_cds_rwa_agg):
    if (rwa_post_crm_amt is None):
        rwa_post_crm_amt = 0.0;

    if (b2_std_gtee_cds_rwa is None):
        b2_std_gtee_cds_rwa = 0.0;

    if (b2_std_gtee_cds_rwa_agg is None):
        b2_std_gtee_cds_rwa_agg = 0.0;

    if ((corep_product_category is not None) and (corep_product_category == "OFF BALANCE SHEET")):
        if (rwa_post_crm_amt <= 0):
            b2_std_rwa_amt_ofbal = rwa_post_crm_amt;

        if (rwa_post_crm_amt > 0):
            if (b2_std_gtee_cds_rwa >= rwa_post_crm_amt):
                b2_std_rwa_amt_ofbal = (b2_std_gtee_cds_rwa / b2_std_gtee_cds_rwa_agg * rwa_post_crm_amt);

            if (b2_std_gtee_cds_rwa < rwa_post_crm_amt):
                b2_std_rwa_amt_ofbal = b2_std_gtee_cds_rwa;
    else:
        b2_std_rwa_amt_ofbal = 0.0;

    return b2_std_rwa_amt_ofbal;
# end of calculate_b2_std_rwa_amt_ofbal()


def calculate_b2_irb_rwa_amt_onbal(aclm_product_type, b2_irb_drawn_rwa, retail_off_bal_sheet_flag,
                                b2_app_rwa_post_crm_amt):
    if (b2_irb_drawn_rwa is None):
        b2_irb_drawn_rwa = 0.0;

    if (b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;

    if (((aclm_product_type is not None) and (aclm_product_type in ('RETAIL', 'ON_BALANCE')))
            and ((retail_off_bal_sheet_flag is not None) and (retail_off_bal_sheet_flag == "Y"))):
        b2_irb_rwa_amt_onbal = b2_irb_drawn_rwa;
    elif ((aclm_product_type is not None) and (aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE',
                                                                    'BASEL-OTC', 'REPO'))):
        b2_irb_rwa_amt_onbal = 0.0;
    else:
        b2_irb_rwa_amt_onbal = b2_app_rwa_post_crm_amt;

    return b2_irb_rwa_amt_onbal;
# end of calculate_b2_irb_rwa_amt_onbal()


def calculate_ret_rwa_post_crm_amt_pct(b2_std_drawn_rwa, b2_std_undrawn_rwa, retail_off_bal_sheet_flag):

    if ((b2_std_drawn_rwa + b2_std_undrawn_rwa) == 0):
        denominator = 1;
    else:
        denominator = (b2_std_drawn_rwa + b2_std_undrawn_rwa);

    if (retail_off_bal_sheet_flag.upper() == "Y"):
        ret_rwa_post_crm_amt_pct = (b2_std_drawn_rwa / denominator);
    else:
        ret_rwa_post_crm_amt_pct = 1;

    return ret_rwa_post_crm_amt_pct;
# end of calculate_ret_rwa_post_crm_amt_pct()


def calculate_b2_std_rwa_post_crm_amt_onbal(corep_product_category, retail_off_bal_sheet_flag,
                                            citizens_override_flag, b2_std_rwa_post_crm_amt,
                                            b2_app_rwa_post_crm_amt, ret_rwa_post_crm_amt_pct, aclm_product_type):
    b2_std_rwa_post_crm_amt_onbal = 0.0;

    if (ret_rwa_post_crm_amt_pct is None):
        ret_rwa_post_crm_amt_pct = 0.0

    if (corep_product_category == 'ON BALANCE SHEET' and retail_off_bal_sheet_flag.upper() == 'Y'):
        if (citizens_override_flag == 'Y'):
            b2_std_rwa_post_crm_amt_onbal = b2_std_rwa_post_crm_amt;
        else:
            b2_std_rwa_post_crm_amt_onbal = b2_app_rwa_post_crm_amt;

        b2_std_rwa_post_crm_amt_onbal = (b2_std_rwa_post_crm_amt_onbal * ret_rwa_post_crm_amt_pct);
    elif (((corep_product_category == 'ON BALANCE SHEET') or (aclm_product_type in ('EQUITY', 'NCA')))
        and (retail_off_bal_sheet_flag != 'Y') and (citizens_override_flag.upper() == 'Y')):
        b2_std_rwa_post_crm_amt_onbal = b2_std_rwa_post_crm_amt;
    elif (((corep_product_category == 'ON BALANCE SHEET') or (aclm_product_type in ('EQUITY', 'NCA')))
        and (retail_off_bal_sheet_flag != 'Y') and (citizens_override_flag.upper() == 'N')):
        b2_std_rwa_post_crm_amt_onbal = b2_app_rwa_post_crm_amt;
    else:
        b2_std_rwa_post_crm_amt_onbal = 0.0;

    return b2_std_rwa_post_crm_amt_onbal;
# end of calculate_b2_std_rwa_post_crm_amt_onbal()


def calculate_b2_std_rwa_amt_onbal(b2_std_rwa_post_crm_amt_onbal, corep_product_category, b2_std_crm_tot_outflow_rwa):
    if (b2_std_rwa_post_crm_amt_onbal is None):
        b2_std_rwa_post_crm_amt_onbal = 0.0

    if (b2_std_crm_tot_outflow_rwa is None):
        b2_std_crm_tot_outflow_rwa = 0.0

    amt_deduction = 0.0

    if (corep_product_category == 'ON BALANCE SHEET'):
        amt_deduction = b2_std_crm_tot_outflow_rwa
    else:
        amt_deduction = 0.0

    b2_std_rwa_amt_onbal = (b2_std_rwa_post_crm_amt_onbal - amt_deduction)

    return b2_std_rwa_amt_onbal
# end of calculate_b2_std_rwa_amt_onbal()


def calculate_b2_irb_rwa_amt_ofbal(aclm_product_type, retail_off_bal_sheet_flag, b2_app_rwa_post_crm_amt,
                                b2_irb_undrawn_rwa):
    b2_irb_rwa_amt_ofbal = 0.0;

    if ((aclm_product_type in ('RETAIL', 'ON_BALANCE') and (retail_off_bal_sheet_flag.upper() == "Y"))):
        b2_irb_rwa_amt_ofbal = b2_irb_undrawn_rwa;
    elif (aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE')):
        b2_irb_rwa_amt_ofbal = b2_app_rwa_post_crm_amt;
    else:
        b2_irb_rwa_amt_ofbal = 0.0;

    return b2_irb_rwa_amt_ofbal;
# end of calculate_b2_irb_rwa_amt_ofbal()


def calculate_default_fund_contrib_indicator(regulatory_product_type):
    if (regulatory_product_type == 'PR_DFP_ESTAR'):
        indicator = 'P';
    elif (regulatory_product_type == 'PR_DFC_ESTAR'):
        indicator = 'C';
    else:
        indicator = 'N';

    return indicator;
# end of calculate_default_fund_contrib_indicator


def calculate_irb_risk_weight_ratio(approved_approach_code, b2_app_ead_post_crm_amt, crr2_501a_discount_flag,
                                    b3_sme_discount_flag, v_infra_discount_factor,
                                    sme_discount_factor, b2_app_rwa_post_crm_amt):
    if ((v_infra_discount_factor is None) or (v_infra_discount_factor == 0)):
        denom_1 = 1;
    else:
        denom_1 = float(v_infra_discount_factor);
        
    if ((sme_discount_factor is None) or (sme_discount_factor == 0)):
        denom_2 = 1;
    else:
        denom_2 = float(sme_discount_factor);

    resultant = 0.0;
    
    if(b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;
    
    if ((approved_approach_code is not None) and (approved_approach_code == 'AIRB')):
        if ((b2_app_ead_post_crm_amt is None) or (b2_app_ead_post_crm_amt == 0)):
            result = 0;
        else:
            if (crr2_501a_discount_flag == 'Y'):
                result = (b2_app_rwa_post_crm_amt / b2_app_ead_post_crm_amt) / denom_1
            elif (b3_sme_discount_flag == 'Y'):
                result = (b2_app_rwa_post_crm_amt / b2_app_ead_post_crm_amt) / denom_2
            else:
                result = (b2_app_rwa_post_crm_amt / b2_app_ead_post_crm_amt)

        if(result is not None):
            op = result;
        else:
            op = 0.0;
            
        if (abs(op) > 9999):
            resultant = 9999;
        else:
            resultant = op;
    else:
        resultant = 0.0;

    return resultant;
# end of calculate_irb_risk_weight_ratio()


def calculate_ead_pd_numerator(b2_app_adjusted_pd_ratio, b2_app_ead_post_crm_amt):
    ead_pd_numerator = 0.0; 

    if ((b2_app_adjusted_pd_ratio * b2_app_ead_post_crm_amt) is None):
        ead_pd_numerator = 0;
    else:
        ead_pd_numerator = (b2_app_adjusted_pd_ratio * b2_app_ead_post_crm_amt);

    return ead_pd_numerator;
# end of calculate_ead_pd_numerator()


def calculate_ead_lgd_numerator(b2_app_adjusted_lgd_ratio, b2_app_ead_post_crm_amt):
    ead_lgd_numerator = 0.0;

    if ((b2_app_adjusted_lgd_ratio * b2_app_ead_post_crm_amt) is None):
        ead_lgd_numerator = 0;
    else:
        ead_lgd_numerator = (b2_app_adjusted_lgd_ratio * b2_app_ead_post_crm_amt);

    return ead_lgd_numerator;
# end of calculate_ead_lgd_numerator()


def calculate_crd4_rep_irbeq_flag(crd4_rep_irbeq_reg_exposure_type, rwa_approach_code, basel_data_flag):
                
    crd4_rep_irbeq_flag=""
    if ((rwa_approach_code == 'AIRB') and (crd4_rep_irbeq_reg_exposure_type == 'Equities') and (basel_data_flag == 'Y')):
        crd4_rep_irbeq_flag = 'Y';
    else:
        crd4_rep_irbeq_flag = 'N';
    return crd4_rep_irbeq_flag;
# end of calculate_crd4_rep_irbeq_flag()


def calculate_b2_irb_ead_amt_onbal(aclm_product_type, retail_off_bal_sheet_flag,
                                b2_irb_drawn_ead, b2_app_ead_post_crm_amt):
    b2_irb_ead_amt_onbal = 0.0;

    if ((aclm_product_type in ('RETAIL', 'ON_BALANCE')) and (retail_off_bal_sheet_flag.upper() == "Y")):
        b2_irb_ead_amt_onbal = b2_irb_drawn_ead;
    elif (aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE', 'BASEL-OTC', 'REPO')):
        b2_irb_ead_amt_onbal = 0.0;
    else:
        b2_irb_ead_amt_onbal = b2_app_ead_post_crm_amt;

    return b2_irb_ead_amt_onbal;
# end of calculate_b2_irb_ead_amt_onbal()


def calculate_b2_irb_ead_amt_ofbal(aclm_product_type, retail_off_bal_sheet_flag,
                                b2_irb_undrawn_ead, b2_app_ead_post_crm_amt):
    b2_irb_ead_amt_onbal = 0.0;

    if ((aclm_product_type in ('RETAIL', 'ON_BALANCE')) and (retail_off_bal_sheet_flag.upper() == "Y")):
        b2_irb_ead_amt_onbal = b2_irb_undrawn_ead;
    elif (aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE')):
        b2_irb_ead_amt_onbal = b2_app_ead_post_crm_amt;
    else:
        b2_irb_ead_amt_onbal = 0.0;

    return b2_irb_ead_amt_onbal;
# end of calculate_b2_irb_ead_amt_ofbal()


def calculate_irb_orig_exp_pre_conv_ofbal(aclm_product_type, bank_base_role_code,
                                        off_bal_led_exp_gbp_post_sec, undrawn_commitment_amt_pst_sec,
                                        off_balance_exposure, undrawn_commitment_amt):
    irb_orig_exp_pre_conv_ofbal = 0.0;

    if (aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE', 'RETAIL', 'ON_BALANCE')):
        if (bank_base_role_code.upper() == "P"):
            irb_orig_exp_pre_conv_ofbal = (off_bal_led_exp_gbp_post_sec + undrawn_commitment_amt_pst_sec);
        else:
            irb_orig_exp_pre_conv_ofbal = (off_balance_exposure + undrawn_commitment_amt);
    else:
        irb_orig_exp_pre_conv_ofbal = 0.0;

    return irb_orig_exp_pre_conv_ofbal;
# end of calculate_irb_orig_exp_pre_conv_ofbal()


def calculate_ead_pre_crm_amt(citizens_override_flag, b2_std_ead_pre_crm_amt, b2_app_ead_pre_crm_amt):
    ead_pre_crm_amt = 0.0;

    if (citizens_override_flag == 'Y'):
        if (b2_std_ead_pre_crm_amt is None):
            ead_pre_crm_amt = 0.0;
        else:
            ead_pre_crm_amt = b2_std_ead_pre_crm_amt;
    else:
        if (b2_app_ead_pre_crm_amt is None):
            ead_pre_crm_amt = 0.0;
        else:
            ead_pre_crm_amt = b2_app_ead_pre_crm_amt;

    return ead_pre_crm_amt;
# end of calculate_ead_pre_crm_amt()


def calculate_b2_irb_el_amt_onbal(aclm_product_type, retail_off_bal_sheet_flag, b2_irb_drawn_expected_loss,
                                b2_app_expected_loss_amt):
    b2_irb_el_amt_onbal = 0.0

    if ((aclm_product_type in ('RETAIL', 'ON_BALANCE')) and (retail_off_bal_sheet_flag == 'Y')):
        b2_irb_el_amt_onbal = b2_irb_drawn_expected_loss
    elif aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE', 'BASEL-OTC', 'REPO'):
        b2_irb_el_amt_onbal = 0.0
    else:
        b2_irb_el_amt_onbal = b2_app_expected_loss_amt

    return b2_irb_el_amt_onbal
# end of calculate_b2_irb_el_amt_onbal()


# def calculate_obligor_type(b2_app_ead_post_crm_amt, external_counterparty_id_temp):
#     obligor_type = ""

#     if (external_counterparty_id_temp == 'UnKnown'):
#         obligor_type_temp = 'RETAIL'
#     else:
#         obligor_type_temp = 'CORP'

#     if (b2_app_ead_post_crm_amt == 0):
#         obligor_type = 'NULL EAD'
#     else:
#         obligor_type = obligor_type_temp

#     return obligor_type
# # end of calculate_obligor_type()


def calculate_lrfe_ufe_flag(fi_avc_category):
    lrfe_ufe_flag = ""

    if (fi_avc_category in ('1', '2')):
        lrfe_ufe_flag = 'Y'
    else:
        lrfe_ufe_flag = 'N'

    return lrfe_ufe_flag;
# end of calculate_lrfe_ufe_flag()


def calculate_irb_orig_exp_pre_conv_onbal(corep_exposure_category, irb_orig_exp_pre_conv_ofbal,
                                        aclm_product_type, b2_irb_orig_exp_pre_con_factor):
    irb_orig_exp_pre_conv_onbal = 0.0

    if (corep_exposure_category is None):
        corep_exposure_category = "";

    if (irb_orig_exp_pre_conv_ofbal is None):
        irb_orig_exp_pre_conv_ofbal = 0.0

    if (b2_irb_orig_exp_pre_con_factor is None):
        b2_irb_orig_exp_pre_con_factor = 0.0

    if ((corep_exposure_category == 'ON BALANCE SHEET') or (aclm_product_type in ('EQUITY', 'NCA'))):
        irb_orig_exp_pre_conv_onbal = (b2_irb_orig_exp_pre_con_factor - irb_orig_exp_pre_conv_ofbal);
    else:
        irb_orig_exp_pre_conv_onbal = 0.0;

    return irb_orig_exp_pre_conv_onbal;
# end of calculate_irb_orig_exp_pre_conv_onbal()


def calculate_rwa_post_crm_pre_sprt_amount(crr2_501a_discount_flag, rwa_post_crm_amount, v_infra_discount_factor,
                                        b3_sme_discount_flag, sme_discount_factor):
    rwa_post_crm_pre_sprt_amount = 0.0;

    if(crr2_501a_discount_flag == 'Y'):
        if(v_infra_discount_factor == None):
            v_infra_discount_factor_temp = 1
        elif(v_infra_discount_factor == 0):
            v_infra_discount_factor_temp = 1
        else:
            v_infra_discount_factor_temp = float(v_infra_discount_factor);

        rwa_post_crm_pre_sprt_amount = (rwa_post_crm_amount / v_infra_discount_factor_temp)
    elif (b3_sme_discount_flag == 'Y'):
        if (sme_discount_factor == None):
            sme_discount_factor_temp = 1
        elif (sme_discount_factor == 0):
            sme_discount_factor_temp = 1
        else:
            sme_discount_factor_temp = sme_discount_factor
        rwa_post_crm_pre_sprt_amount = (rwa_post_crm_amount / sme_discount_factor_temp)
    else:
        rwa_post_crm_pre_sprt_amount = rwa_post_crm_amount

    return rwa_post_crm_pre_sprt_amount;
# end of calculate_rwa_post_crm_pre_sprt_amount()


def calculate_b2_irb_rwa_pre_sprt_amt_onbal(crr2_501a_discount_flag, b2_irb_rwa_amt_onbal, v_infra_discount_factor,
                                            b3_sme_discount_flag, sme_discount_factor):
    b2_irb_rwa_pre_sprt_amt_onbal = 0.0;

    if (crr2_501a_discount_flag == 'Y'):
        if (v_infra_discount_factor is None or v_infra_discount_factor == 0):
            v_infra_discount_factor_temp = 1
        else:
            v_infra_discount_factor_temp = float(v_infra_discount_factor);
        b2_irb_rwa_pre_sprt_amt_onbal = b2_irb_rwa_amt_onbal / v_infra_discount_factor_temp
    elif (b3_sme_discount_flag == 'Y'):
        if (sme_discount_factor is None or sme_discount_factor == 0):
            sme_discount_factor_temp = 1
        else:
            sme_discount_factor_temp = sme_discount_factor
        b2_irb_rwa_pre_sprt_amt_onbal = (b2_irb_rwa_amt_onbal / sme_discount_factor_temp)
    else:
        b2_irb_rwa_pre_sprt_amt_onbal = b2_irb_rwa_amt_onbal

    return b2_irb_rwa_pre_sprt_amt_onbal;
# end of calculate_b2_irb_rwa_pre_sprt_amt_onbal()


def calculate_b2_irb_rwa_pre_sprt_amt_ofbal(crr2_501a_discount_flag, b2_irb_rwa_amt_ofbal, v_infra_discount_factor,
                                            b3_sme_discount_flag, sme_discount_factor):
    b2_irb_rwa_pre_sprt_amt_ofbal = 0.0

    if (crr2_501a_discount_flag == 'Y'):
        if ((v_infra_discount_factor is None) or (v_infra_discount_factor == 0)):
            v_infra_discount_factor_temp = 1
        else:
            v_infra_discount_factor_temp = float(v_infra_discount_factor);
        b2_irb_rwa_pre_sprt_amt_ofbal = (b2_irb_rwa_amt_ofbal / v_infra_discount_factor_temp)
    elif (b3_sme_discount_flag == 'Y'):
        if (sme_discount_factor is None or sme_discount_factor == 0):
            sme_discount_factor_temp = 1
        else:
            sme_discount_factor_temp = sme_discount_factor
        b2_irb_rwa_pre_sprt_amt_ofbal = b2_irb_rwa_amt_ofbal / sme_discount_factor_temp
    else:
        b2_irb_rwa_pre_sprt_amt_ofbal = b2_irb_rwa_amt_ofbal

    return b2_irb_rwa_pre_sprt_amt_ofbal;
# end of calculate_b2_irb_rwa_pre_sprt_amt_ofbal()


def calculate_b2_irb_el_amt_ofbal(aclm_product_type, retail_off_bal_sheet_flag, b2_irb_undrawn_expected_loss,
                                b2_app_expected_loss_amt):
    b2_irb_el_amt_ofbal = 0.0
    if((aclm_product_type in ('RETAIL', 'ON_BALANCE')) and (retail_off_bal_sheet_flag == 'Y')) :

        if  not b2_irb_undrawn_expected_loss :
            b2_irb_undrawn_expected_loss = 0.0
            #print(b2_irb_undrawn_expected_loss)

        b2_irb_el_amt_ofbal = b2_irb_undrawn_expected_loss

    elif(aclm_product_type in ('UNDRAWN-COMM', 'OFF_BALANCE')):

        if  not  b2_app_expected_loss_amt :
            b2_app_expected_loss_amt = 0.0


        b2_irb_el_amt_ofbal = b2_app_expected_loss_amt;

    return b2_irb_el_amt_ofbal;
# end of calculate_b2_irb_el_amt_ofbal()


def calculate_retail_obligor_count(obligor_type, obligors_count):
    retail_obligor_count = 0;

    if(obligor_type == 'RETAIL'):

        if not obligors_count:
            obligors_count = 0;

        retail_obligor_count = obligors_count;

    return retail_obligor_count;
# end of calculate_retail_obligor_count()


def calculate_pd_band_code(division_sub2, approved_approach_code, dim_pd_band_code):
    pd_band_code_final = '-1'

    sub_list = ['CITIZENS2001', 'CITIZENS2002', 'CITIZENS2003', 'CITIZENS2001_NC', 'CITIZENS2002_NC', 'CITIZENS2003_NC']

    if (division_sub2 in sub_list):
        pd_band_code_temp2 = 'STD'
    else:
        pd_band_code_temp2 = approved_approach_code

    if (pd_band_code_temp2 == 'AIRB'):
        if not dim_pd_band_code:
            dim_pd_band_code = '-1'

        pd_band_code_final = dim_pd_band_code

    return pd_band_code_final
# end of calculate_pd_band_code()


def calculate_mgs(division_sub2, approved_approach_code, dim_mgs):
    mgs_final = -1

    sub_list = ['CITIZENS2001', 'CITIZENS2002', 'CITIZENS2003', 'CITIZENS2001_NC', 'CITIZENS2002_NC', 'CITIZENS2003_NC']

    if (division_sub2 in sub_list):
        mgs_temp2 = 'STD'
    else:
        mgs_temp2 = approved_approach_code

    if (mgs_temp2 == 'AIRB'):
        if not dim_mgs:
            dim_mgs = -1

        mgs_final = dim_mgs

    return mgs_final
# end of calculate_mgs()


def calculate_sme_flag(b2_irb_asset_class_reg_exposure_sub_type):
    sme_flag = "N";

    if (b2_irb_asset_class_reg_exposure_sub_type == "Corporates with turnover < Eur 50M"):
        sme_flag = "Y";
    elif (b2_irb_asset_class_reg_exposure_sub_type == "Retail SME"):
        sme_flag = "Y";
    else:
        sme_flag = "N";

    return sme_flag;
# end of calculate_sme_flag


def calculate_slotting_residual_maturity(residual_maturity_days):
    slotting_residual_maturity = "";

    if (residual_maturity_days is None):
        slotting_residual_maturity = ''
    else:
        if (residual_maturity_days < 913):
            slotting_residual_maturity = '< 2.5 YEARS';
        else:
            slotting_residual_maturity = '>= 2.5 YEARS';

    return slotting_residual_maturity;
# end of calculate_slotting_residual_maturity


def calculate_risk_type(reporting_type_code, aclm_product_type):
    if (reporting_type_code is None):
        reporting_type_code = "X";

    if ((reporting_type_code not in ('MR', 'OR', 'SR', 'NC')) and (aclm_product_type not in ('BASEL-OTC', 'REPO'))):
        risk_type = "CRD";
    elif ((aclm_product_type not in ('BASEL-OTC', 'REPO')) and (reporting_type_code not in ('MR', 'OR', 'NC'))):
        risk_type = "CCR";
    elif (reporting_type_code == "NC"):
        risk_type = "NCA";
    elif (reporting_type_code == "OR"):
        risk_type = "OP";
    elif ((reporting_type_code == "MR") and (aclm_product_type in ('CADII', 'SPEC'))):
        risk_type = "MAR";
    else:
        risk_type = "";

    return risk_type;
# end calculate_risk_type


def calculate_finrep_counterparty_sector(rwa_approach_code, b2_std_asset_class_basel_exposure_type, sub_sector,
                                        b2_irb_asset_class_reg_exposure_type):
    finrep_counterparty_sector = "";
    
    if (rwa_approach_code == 'STD'):
        if ((b2_std_asset_class_basel_exposure_type == 'Exposures to central governments or central banks') 
            and (sub_sector == 'Central Government')):
            finrep_counterparty_sector = 'Central governments'
        elif (b2_std_asset_class_basel_exposure_type == 'Exposures to regional governments or local authorities' and sub_sector == 'Local Authorities'):
            finrep_counterparty_sector = 'Regional governments or local authorities'
        elif (b2_std_asset_class_basel_exposure_type == 'Exposures to public sector entities' and sub_sector == 'Other public bodies'):
            finrep_counterparty_sector = 'Public sector entities'
        elif (b2_std_asset_class_basel_exposure_type == 'Exposures to international organisations'):
            finrep_counterparty_sector = 'International Organisations'
        else:
            finrep_counterparty_sector = 'Others'
    elif (rwa_approach_code == 'AIRB'):
        if (b2_irb_asset_class_reg_exposure_type == 'Central governments and central banks' and b2_std_asset_class_basel_exposure_type == 'Exposures to central governments or central banks'):
            finrep_counterparty_sector = 'Central governments'
        elif (b2_irb_asset_class_reg_exposure_type == 'Institutions' and b2_std_asset_class_basel_exposure_type == 'Exposures to regional governments or local authorities'):
            finrep_counterparty_sector = 'Regional governments or local authorities [Institutions]'
        elif (b2_irb_asset_class_reg_exposure_type == 'Central governments and central banks' and b2_std_asset_class_basel_exposure_type == 'Exposures to public sector entities'):
            finrep_counterparty_sector = 'Public sector entities [Central governments and central banks]'
        elif (b2_irb_asset_class_reg_exposure_type == 'Institutions' and b2_std_asset_class_basel_exposure_type == 'Exposures to public sector entities'):
            finrep_counterparty_sector = 'Public sector entities [Institutions]'
        elif (b2_irb_asset_class_reg_exposure_type == 'Central governments and central banks' and b2_std_asset_class_basel_exposure_type == 'Exposures to international organisations'):
            finrep_counterparty_sector = 'International Organisations [Central governments and central banks]'
        else:
            finrep_counterparty_sector = 'Others'
    else:
        finrep_counterparty_sector = 'Others'

    return finrep_counterparty_sector;
# end of calculate_finrep_counterparty_sector


def calculate_c33_exposure_sector(ec92_code, bsd_marker, external_counterparty_id_temp, sector_cluster, sub_sector):
    
    if ((ec92_code in ('G', 'H') and bsd_marker in ('Q', 'P', 'S')) or (ec92_code == 'J' and bsd_marker == 'C')):
        c33_exposure_sector = 'General Government'
    elif ((external_counterparty_id_temp == 'UnKnown') and (
            sector_cluster == 'Sovereigns &' or ' Quasi Sovereigns') and (
                sub_sector in ('Local Authorities', 'Other public bodies', 'Central Government'))):
        c33_exposure_sector = 'General Government'
    else:
        c33_exposure_sector = 'Others'
        
    return c33_exposure_sector;
# end of calculate_c33_exposure_sector


def calculate_crd4_rep_irb_flag(b2_irb_asset_class_reg_exposure_type, rwa_calc_method, corep_irb_exposure_sub_type,
                                b2_irb_asset_class_basel_exposure_sub_type, basel_data_flag, rwa_approach_code,
                                dac_type, dac_valid_flag):
    crd4_rep_irb_flag = 'N'
    derived_asset_class_rk_2 = 0.0
    derived_asset_class_rk = 0.0

    if (b2_irb_asset_class_reg_exposure_type in ('Central Governments and Central Banks', 'Institutions',
                                                'Non-credit Obligation Assets', 'Equities',
                                                'Securitisation Positions')):
        derived_asset_class_rk = b2_irb_asset_class_basel_exposure_sub_type;
    elif ((b2_irb_asset_class_reg_exposure_type == 'Corporates') and (rwa_calc_method == 'AIRB - SL PD/LGD Approach')):
        derived_asset_class_rk = 'RWA Calc Under AIRB - SL PD/LGD';
    elif ((b2_irb_asset_class_reg_exposure_type == 'Corporates') and (
            rwa_calc_method == 'AIRB - SL Slotting Approach')):
        derived_asset_class_rk = 'RWA Calc Under AIRB - SL Slotting';
    elif (b2_irb_asset_class_reg_exposure_type == 'RET'):
        derived_asset_class_rk = COREP_IRB_EXPOSURE_SUB_TYPE;
    else:
        derived_asset_class_rk = b2_irb_asset_class_basel_exposure_sub_type;

    if ((b2_irb_asset_class_basel_exposure_sub_type == derived_asset_class_rk)
            and (dac_type == 'COREP_IRB') and (dac_valid_flag == 'Y')):
        derived_asset_class_rk_2 = b2_irb_asset_class_reg_exposure_type;

    if (derived_asset_class_rk_2 is None):
        derived_asset_class_rk_2 = 'XXXX';

    if (rwa_approach_code == 'AIRB' and derived_asset_class_rk_2 not in
            ('Equities', 'Non-credit Obligation Assets', 'Securitisation Positions') and basel_data_flag == 'Y'):
        crd4_rep_irb_flag = 'Y'

    return crd4_rep_irb_flag;
# end of calculate_crd4_rep_irb_flag()


def calculate_crd4_memo_items_exposure_type(b2_std_asset_class_basel_exposure_type,
                                            b2_std_pre_dflt_asset_class_basel_exposure_type,
                                            b2_irb_asset_class_reg_exposure_type):
    crd4_memo_items_exposure_type = "";

    if (b2_std_asset_class_basel_exposure_type == "Exposures in default"):
        if ((b2_std_pre_dflt_asset_class_basel_exposure_type == "Exposures Secured by Immovable Property") or
                (b2_std_pre_dflt_asset_class_basel_exposure_type == "N/A")):
            crd4_memo_items_exposure_type = b2_irb_asset_class_reg_exposure_type;
        elif ((b2_std_pre_dflt_asset_class_basel_exposure_type == "Exposures Secured by Immovable Property") or
            (b2_std_pre_dflt_asset_class_basel_exposure_type != "N/A")):
            crd4_memo_items_exposure_type = b2_std_pre_dflt_asset_class_basel_exposure_type;
    elif (b2_std_asset_class_basel_exposure_type == "Exposures Secured by Immovable Property"):
            crd4_memo_items_exposure_type = b2_irb_asset_class_reg_exposure_type;
    else:
        crd4_memo_items_exposure_type = b2_irb_asset_class_reg_exposure_type;

    return crd4_memo_items_exposure_type;
# end of calculate_crd4_memo_items_exposure_type


def calculate_b2_irb_obsrvd_new_default_flag(external_counterparty_id_temp, approved_approach_code,
                                            b2_app_adjusted_pd_ratio, airb_pre_dflt):
    if b2_app_adjusted_pd_ratio == None:
        b2_app_adjusted_pd_ratio_temp = 0.02501
    else:
        b2_app_adjusted_pd_ratio_temp = b2_app_adjusted_pd_ratio;
        
    if airb_pre_dflt == None:
        airb_pre_dflt_temp = 'Y'
    else:
        airb_pre_dflt_temp = airb_pre_dflt;
        
    if (external_counterparty_id_temp is None or external_counterparty_id_temp == 'UnKnown'):
        b2_irb_obsrvd_new_default_flag = 'N'
    elif (approved_approach_code == 'AIRB' and b2_app_adjusted_pd_ratio_temp == 1 and airb_pre_dflt_temp != 'Y'):
        b2_irb_obsrvd_new_default_flag = 'Y'
    else:
        b2_irb_obsrvd_new_default_flag = 'N'

    return b2_irb_obsrvd_new_default_flag;
# end calculate_b2_irb_obsrvd_new_default_flag()


def calculate_b2_std_obsrvd_new_default_flag(external_counterparty_id_temp, approved_approach_code,
                                            b2_irb_asset_class_basel_exposure_type, std_pre_dflt):
    if std_pre_dflt is None:
        std_pre_dflt_temp = 'Y'
    else:
        std_pre_dflt_temp = std_pre_dflt
        
    if (external_counterparty_id_temp == 'UnKnown'):
        b2_std_obsrvd_new_default_flag = 'N'
    elif ((approved_approach_code == 'STD') and (b2_irb_asset_class_basel_exposure_type == 'Exposures in default')
        and (std_pre_dflt_temp != 'Y')):
        b2_std_obsrvd_new_default_flag = 'Y'
    else:
        b2_std_obsrvd_new_default_flag = 'N'

    return b2_std_obsrvd_new_default_flag;
# end calculate_b2_std_obsrvd_new_default_flag()


def calculate_crd4_memo_items_asset_class_basel_exposure_type(memo_b2_std_basel_exposure_type,
                                            crd4_memo_items_exposure_type, memo_b2_std_type):
    memo_b2_std_basel_exposure_type = '';

    if (crd4_memo_items_exposure_type is None):
        crd4_memo_items_exposure_type = '';
        
    if (crd4_memo_items_exposure_type == 'Equities'):
        memo_b2_std_basel_exposure_type = 'Equity Claims';
    elif (crd4_memo_items_exposure_type == 'Central Governments and Central Banks'):
        memo_b2_std_basel_exposure_type = 'Exposures to central governments or central banks';
    elif (crd4_memo_items_exposure_type == 'Corporates'):
        memo_b2_std_basel_exposure_type = 'Exposures to corporates';
    elif (crd4_memo_items_exposure_type == 'Institutions'):
        memo_b2_std_basel_exposure_type = 'Exposures to institutions';
    elif (crd4_memo_items_exposure_type == 'Non-credit Obligation Assets'):
        memo_b2_std_basel_exposure_type = 'Other items';
    elif (crd4_memo_items_exposure_type == 'Retail'):
        memo_b2_std_basel_exposure_type = 'Retail exposures';
    else:
        memo_b2_std_basel_exposure_type = crd4_memo_items_exposure_type;
        
    if(memo_b2_std_basel_exposure_type is not None and memo_b2_std_type == "B2_STD"):        
        result = memo_b2_std_basel_exposure_type
    else:
        result = 'N/A'

    return result;
#   end calculate_b2_std_obsrvd_new_default_flag()


def calculate_std_pre_dflt(rwa_approach_code_qtr, memo_b2_std_basel_exposure_type_qtr):
    if (rwa_approach_code_qtr == "STD" and memo_b2_std_basel_exposure_type_qtr == "Exposures in default"):
        std_pre_dflt_temp = 'Y'
    else:
        std_pre_dflt_temp = 'N'

    return std_pre_dflt_temp;
# end of calculate_std_pre_dflt()


def calculate_airb_pre_dflt(rwa_approach_code_qtr, b2_app_adjusted_pd_ratio_qtr):
    if (rwa_approach_code_qtr == "AIRB" and b2_app_adjusted_pd_ratio_qtr == 1):
        airb_pre_dflt_temp = 'Y'
    else:
        airb_pre_dflt_temp = 'N'

    return airb_pre_dflt_temp
# end of calculate_airb_pre_dflt()


def calculate_crd4_memo_items_asset_class_reg_exposure_type(memo_b2_std_reg_exposure_type, 
                                                            memo_b2_std_basel_exposure_type, 
                                                            crd4_memo_items_exposure_type, memo_b2_std_type):
    memo_b2_std_basel_exposure_type = '';
    memo_b2_std_reg_exposure_type = '';
    
    if (crd4_memo_items_exposure_type is None):
        crd4_memo_items_exposure_type = '';
        
    if (crd4_memo_items_exposure_type == 'Equities'):
        memo_b2_std_basel_exposure_type = 'Equity Claims';
    elif (crd4_memo_items_exposure_type == 'Central Governments and Central Banks'):
        memo_b2_std_basel_exposure_type = 'Exposures to central governments or central banks';
    elif (crd4_memo_items_exposure_type == 'Corporates'):
        memo_b2_std_basel_exposure_type = 'Exposures to corporates';
    elif (crd4_memo_items_exposure_type == 'Institutions'):
        memo_b2_std_basel_exposure_type = 'Exposures to institutions';
    elif (crd4_memo_items_exposure_type == 'Non-credit Obligation Assets'):
        memo_b2_std_basel_exposure_type = 'Other items';
    elif (crd4_memo_items_exposure_type == 'Retail'):
        memo_b2_std_basel_exposure_type = 'Retail exposures';
    else:
        memo_b2_std_basel_exposure_type = crd4_memo_items_exposure_type;
        
    if(memo_b2_std_basel_exposure_type is not None and memo_b2_std_type == "B2_STD"):        
        result = memo_b2_std_reg_exposure_type
    else:
        result = None

    return result;
#   end of calculate_crd4_memo_items_asset_class_reg_exposure_type()

    
def calculate_crd4_memo_items_asset_class_basel_exposure_sub_type(memo_b2_std_basel_exposure_type,
                                                                crd4_memo_items_exposure_type, memo_b2_std_type, 
                                                                memo_b2_std_basel_exposure_sub_type):
    memo_b2_std_basel_exposure_type = '';
    memo_b2_std_basel_exposure_sub_type = '';

    if (crd4_memo_items_exposure_type is None):
        crd4_memo_items_exposure_type = '';
        
    if (crd4_memo_items_exposure_type == 'Equities'):
        memo_b2_std_basel_exposure_type = 'Equity Claims';
    elif (crd4_memo_items_exposure_type == 'Central Governments and Central Banks'):
        memo_b2_std_basel_exposure_type = 'Exposures to central governments or central banks';
    elif (crd4_memo_items_exposure_type == 'Corporates'):
        memo_b2_std_basel_exposure_type = 'Exposures to corporates';
    elif (crd4_memo_items_exposure_type == 'Institutions'):
        memo_b2_std_basel_exposure_type = 'Exposures to institutions';
    elif (crd4_memo_items_exposure_type == 'Non-credit Obligation Assets'):
        memo_b2_std_basel_exposure_type = 'Other items';
    elif (crd4_memo_items_exposure_type == 'Retail'):
        memo_b2_std_basel_exposure_type = 'Retail exposures';
    else:
        memo_b2_std_basel_exposure_type = crd4_memo_items_exposure_type;
        
    if(memo_b2_std_basel_exposure_type is not None and memo_b2_std_type == "B2_STD"):        
        result = memo_b2_std_basel_exposure_sub_type
    else:
        result = 'N/A'

    return result;
#   end calculate_crd4_memo_items_asset_class_basel_exposure_sub_type

def calculate_ra_ad_exp(aclm_product_type, b2_app_ead_post_crm_amt, aiml_product_type, cost_centre, bspl_900_gl_code, net_exposure, bspl_60_gl_code, off_balance_exposure, 
drawn_ledger_balance, b2_irb_interest_at_default_gbp,accrued_interest_gbp, approved_approach_code, rep_provision_amt_gbp , e_star_gross_exposure, basel_data_flag , internal_transaction_flag):
    ra_ad_exp = 0.0

    if(b2_app_ead_post_crm_amt is None):
        b2_app_ead_post_crm_amt = 0.0
        
    if(net_exposure is None):
        net_exposure = 0.0
        
    if(drawn_ledger_balance is None):
        drawn_ledger_balance = 0.0
        
    if(accrued_interest_gbp is None):
        accrued_interest_gbp = 0.0
        
    if(e_star_gross_exposure is None):
        e_star_gross_exposure = 0.0
        
    if(rep_provision_amt_gbp is None):
        rep_provision_amt_gbp = 0.0 

    if(b2_irb_interest_at_default_gbp is None):
        b2_irb_interest_at_default_gbp = 0.0

    if(cost_centre is None):
        cost_centre = ""

    if(bspl_900_gl_code is None):
        bspl_900_gl_code = ""

    if(aiml_product_type is None):
        aiml_product_type = ""

    if(aclm_product_type is None):
        aclm_product_type = ""

    if(bspl_60_gl_code is None):
        bspl_60_gl_code = ""

    if(approved_approach_code is None):
        approved_approach_code = ""

    if(internal_transaction_flag is None):
        internal_transaction_flag = ""

    if(off_balance_exposure is None):
        off_balance_exposure = 0.0

    if(basel_data_flag is None):
        basel_data_flag = ""

    
    if(aclm_product_type == 'NCA'):
        ra_ad_exp1 = float(b2_app_ead_post_crm_amt)

    elif(aclm_product_type in ('OFF_BALANCE', 'UNDRAWN-COMM')):
        ra_ad_exp1 = 0.0

    elif(aiml_product_type in('Term Loan', 'Revolver Receivable') or  (cost_centre +
        bspl_900_gl_code in ('RBSSUBR18013100000','150005412013100000','RBSSUBR04013100000','RBSSUBL01013100000','RBSSUB110013100000','150021259013100000'))):
        ra_ad_exp1 = float(net_exposure)

    else:
        if((bspl_60_gl_code == 'XCONTLIAB') and ABS (off_balance_exposure) > 0):
            ra_ad_exp1 = 0.0
        else:
            ra_ad_exp1 = float(drawn_ledger_balance)
    
    if(approved_approach_code == 'AIRB'):
        ra_ad_exp2 = float(b2_irb_interest_at_default_gbp)

    elif(approved_approach_code == 'STD'):
        ra_ad_exp2 = float(accrued_interest_gbp)

    else:
        ra_ad_exp2 = 0.0
        
    if (cost_centre in('OVERSGMPF','OVERS7GMP','OVERS7GIB','NCOREZUE1', 'NCOREGMP1','GREENZUEF','GREENCAPP','GREENCAPH', 'GREENCHUE','OVERS7HMP') and internal_transaction_flag == 'N'
                        and basel_data_flag == 'Y'):
        ra_ad_exp3 = float(e_star_gross_exposure)

    else:
        ra_ad_exp3 = 0.0
        
    ra_ad_exp = (float(ra_ad_exp1) + float(ra_ad_exp2) + float(ra_ad_exp3)) - float(rep_provision_amt_gbp)

    return ra_ad_exp

#	end calculate_ra_ad_exp()

###############################################################
#########-----------Sub Exposures Functions-----------########
###############################################################

def calculate_b2_irb_crm_ufcp_guar_amt(corep_crm_category, sec_discntd_b2_irb_garntee_amt,
                                    double_default_flag, b2_irb_gtee_amt):
    b2_irb_crm_ufcp_guar_amt = 0.0;

    if (corep_crm_category.upper() == "GUARANTEES"):
        if (double_default_flag.upper() != "Y"):
            if (sec_discntd_b2_irb_garntee_amt == 0):
                if (b2_irb_gtee_amt is None):
                    b2_irb_gtee_amt = 0.0;
                b2_irb_crm_ufcp_guar_amt = b2_irb_gtee_amt;
            else:
                if (sec_discntd_b2_irb_garntee_amt is None):
                    sec_discntd_b2_irb_garntee_amt = 0.0;
                b2_irb_crm_ufcp_guar_amt = sec_discntd_b2_irb_garntee_amt;
        else:
            b2_irb_crm_ufcp_guar_amt = 0.0;
    else:
        b2_irb_crm_ufcp_guar_amt = 0.0;

    return b2_irb_crm_ufcp_guar_amt;
# end of calculate_b2_irb_crm_ufcp_guar_amt()


def calculate_b2_irb_crm_ufcp_cred_deriv_amt(corep_crm_category, mitigant_type,sec_discntd_b2_irb_garntee_amt,
                                            double_default_flag, b2_irb_gtee_amt):
    b2_irb_crm_ufcp_cred_deriv_amt = 0.0;

    if (corep_crm_category.upper() == "CREDIT DERIVATIVES"):
        if (mitigant_type.upper() == 'CD'):
            if (double_default_flag.upper() != "Y"):
                if (sec_discntd_b2_irb_garntee_amt == 0):
                    if (b2_irb_gtee_amt is None):
                        b2_irb_gtee_amt = 0.0;
                    b2_irb_crm_ufcp_cred_deriv_amt = b2_irb_gtee_amt;
                else:
                    if (sec_discntd_b2_irb_garntee_amt is None):
                        sec_discntd_b2_irb_garntee_amt = 0.0;
                    b2_irb_crm_ufcp_cred_deriv_amt = sec_discntd_b2_irb_garntee_amt;

    return b2_irb_crm_ufcp_cred_deriv_amt;
# end of calculate_b2_irb_crm_ufcp_cred_deriv_amt()


def calculate_crm_ufcp_cred_deriv_amt(corep_crm_category, double_default_flag, mitigant_type, b2_irb_gtee_amt):
    crm_ufcp_cred_deriv_amt = 0.0;

    if (corep_crm_category == 'CREDIT DERIVATIVES'):
        if (double_default_flag == 'Y'):
            if (mitigant_type == 'CD'):
                if (b2_irb_gtee_amt is None):
                    b2_irb_gtee_amt = 0.0;

                crm_ufcp_cred_deriv_amt_temp3 = b2_irb_gtee_amt;
            else:
                crm_ufcp_cred_deriv_amt_temp3 = 0.0;
            crm_ufcp_cred_deriv_amt_temp2 = crm_ufcp_cred_deriv_amt_temp3
        else:
            crm_ufcp_cred_deriv_amt_temp2 = 0.0;
        crm_ufcp_cred_deriv_amt = crm_ufcp_cred_deriv_amt_temp2

    return crm_ufcp_cred_deriv_amt
# end of calculate_crm_ufcp_cred_deriv_amt


def calculate_crm_ufcp_guar_amt(corep_crm_category, double_default_flag, b2_irb_gtee_amt):
    crm_ufcp_guar_amt = 0.0;

    if (b2_irb_gtee_amt is None):
        b2_irb_gtee_amt = 0.0;

    if ((double_default_flag is not None) and (double_default_flag == 'Y')):
        crm_ufcp_guar_amt_temp2 = b2_irb_gtee_amt;
    else:
        crm_ufcp_guar_amt_temp2 = 0.0;

    if ((corep_crm_category is not None) and (corep_crm_category == 'GUARANTEES')):        
        crm_ufcp_guar_amt = crm_ufcp_guar_amt_temp2;

    return crm_ufcp_guar_amt;
# end of calculate_crm_ufcp_guar_amt


def calculate_crm_fcp_elig_fin_coll_amt(corep_crm_category, double_default_flag,b2_irb_coll_amt):
    crm_fcp_elig_fin_coll_amt = 0.0;

    if (double_default_flag == 'Y'):
            crm_fcp_elig_fin_coll_amt_temp2 = 0.0
    else:
        if (b2_irb_coll_amt is None):
            b2_irb_coll_amt = 0.0

        crm_fcp_elig_fin_coll_amt_temp2 = b2_irb_coll_amt



    if (corep_crm_category == 'ELIGIBLE FINANCIAL COLLATERAL'):
        crm_fcp_elig_fin_coll_amt = crm_fcp_elig_fin_coll_amt_temp2


    return crm_fcp_elig_fin_coll_amt;

# end of calculate_crm_fcp_elig_fin_coll_amt


def calculate_b2_irb_crm_fcp_oth_funded_amt(corep_crm_category, sec_discntd_b2_irb_colltrl_amt,
                                            double_default_flag, b2_irb_coll_amt):
    b2_irb_crm_fcp_oth_funded_amt = 0.0;

    if (sec_discntd_b2_irb_colltrl_amt is None):
        sec_discntd_b2_irb_colltrl_amt = 0.0;

    if not double_default_flag:
        double_default_flag = 'N'

    if (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION' and sec_discntd_b2_irb_colltrl_amt != 0 and double_default_flag != 'Y'):
        b2_irb_crm_fcp_oth_funded_amt = sec_discntd_b2_irb_colltrl_amt
    elif (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION' and sec_discntd_b2_irb_colltrl_amt == 0 and double_default_flag != 'Y'):
        b2_irb_crm_fcp_oth_funded_amt = b2_irb_coll_amt
    else:
        b2_irb_crm_fcp_oth_funded_amt = 0.0;
    return b2_irb_crm_fcp_oth_funded_amt;
# end of calculate_b2_irb_crm_fcp_oth_funded_amt


def calculate_b2_irb_crm_fcp_elg_fin_col_amt(crm_fcp_elig_fin_coll_amt_agg,b2_irb_netted_coll_gbp):
    b2_irb_crm_fcp_elg_fin_col_amt = 0.0;

    if (crm_fcp_elig_fin_coll_amt_agg is None):
        crm_fcp_elig_fin_coll_amt_agg = 0.0;

    if (b2_irb_netted_coll_gbp is None):
        b2_irb_netted_coll_gbp = 0.0;

    b2_irb_crm_fcp_elg_fin_col_amt = (crm_fcp_elig_fin_coll_amt_agg + b2_irb_netted_coll_gbp)

    return b2_irb_crm_fcp_elg_fin_col_amt
# end of calculate_b2_irb_crm_fcp_elg_fin_col_amt()


def calculate_b2_irb_crm_fcp_real_estate_amt(corep_crm_category, double_default_flag, b2_irb_coll_amt):
    b2_irb_crm_fcp_real_estate_amt = 0.0

    if (corep_crm_category == 'REAL ESTATE'):
        if (double_default_flag == 'Y'):
            b2_irb_crm_fcp_real_estate_amt_tmp = 0.0
        else:
            if (b2_irb_coll_amt is None):
                b2_irb_coll_amt = 0.0

            b2_irb_crm_fcp_real_estate_amt_tmp = b2_irb_coll_amt

        b2_irb_crm_fcp_real_estate_amt = b2_irb_crm_fcp_real_estate_amt_tmp

    return b2_irb_crm_fcp_real_estate_amt
# end of calculate_b2_irb_crm_fcp_real_estate_amt


def calculate_b2_irb_crm_fcp_oth_phy_col_amt(corep_crm_category, double_default_flag, b2_irb_coll_amt):
    b2_irb_crm_fcp_oth_phy_col_amt = 0.0

    if (corep_crm_category == 'OTHER PHYSICAL COLLATERAL'):
        if (double_default_flag == 'Y'):
            b2_irb_crm_fcp_oth_phy_col_amt_tmp = 0.0
        else:
            if (b2_irb_coll_amt is None):
                b2_irb_coll_amt = 0.0

            b2_irb_crm_fcp_oth_phy_col_amt_tmp = b2_irb_coll_amt

        b2_irb_crm_fcp_oth_phy_col_amt = b2_irb_crm_fcp_oth_phy_col_amt_tmp

    return b2_irb_crm_fcp_oth_phy_col_amt
# end of calculate_b2_irb_crm_fcp_oth_phy_col_amt()


def calculate_b2_irb_crm_fcp_receivables_amt(corep_crm_category, double_default_flag, b2_irb_coll_amt):
    b2_irb_crm_fcp_receivables_amt = 0.0

    if (corep_crm_category == 'RECEIVABLES'):
        if (double_default_flag == 'Y'):
            b2_irb_crm_fcp_receivables_amt_tmp = 0.0
        else:
            if (b2_irb_coll_amt is None):
                b2_irb_coll_amt = 0.0

            b2_irb_crm_fcp_receivables_amt_tmp = b2_irb_coll_amt

        b2_irb_crm_fcp_receivables_amt = b2_irb_crm_fcp_receivables_amt_tmp

    return b2_irb_crm_fcp_receivables_amt
# end of calculate_b2_irb_crm_fcp_receivables_amt()


def calculate_b2_irb_crm_ufcp_double_def_amt(crm_ufcp_guar_amt_agg,crm_ufcp_cred_deriv_amt_agg):

    b2_irb_crm_ufcp_double_def_amt = 0.0;
    if (crm_ufcp_guar_amt_agg is None):
        crm_ufcp_guar_amt_agg = 0.0;

    if (crm_ufcp_cred_deriv_amt_agg is None):
        crm_ufcp_cred_deriv_amt_agg = 0.0;

    b2_irb_crm_ufcp_double_def_amt = (crm_ufcp_guar_amt_agg + crm_ufcp_cred_deriv_amt_agg)

    return b2_irb_crm_ufcp_double_def_amt
# end of calculate_b2_irb_crm_ufcp_double_def_amt()


def calculate_b2_std_eff_fin_coll_amt(corep_crm_category, b2_std_eff_coll_amt_post_hcut):
    b2_std_eff_fin_coll_amt = 0.0;

    if (corep_crm_category in ('ELIGIBLE FINANCIAL COLLATERAL')):
        if not b2_std_eff_coll_amt_post_hcut:
            b2_std_eff_coll_amt_post_hcut = 0.0;
        b2_std_eff_fin_coll_amt = b2_std_eff_coll_amt_post_hcut;
    else:
        b2_std_eff_fin_coll_amt = 0.0;

    return b2_std_eff_fin_coll_amt
# end of calculate_b2_std_eff_fin_coll_amt()


def calculate_b2_std_oth_fun_cred_prot_amt(corep_crm_category, b2_std_eff_coll_amt_post_hcut):
    b2_std_oth_fun_cred_prot_amt = 0.0;

    if (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION'):
        if (b2_std_eff_coll_amt_post_hcut is None):
            b2_std_eff_coll_amt_post_hcut = 0.0;
        b2_std_oth_fun_cred_prot_amt = b2_std_eff_coll_amt_post_hcut;
    else:
        b2_std_oth_fun_cred_prot_amt = 0.0;

    return b2_std_oth_fun_cred_prot_amt;
# end of calculate_b2_std_oth_fun_cred_prot_amt()


def calculate_b2_std_coll_fx_haircut_amt(b2_std_fx_haircut, b2_std_elig_coll_amt):
    b2_std_coll_fx_haircut_amt = 0.0;

    if b2_std_fx_haircut is None:
        b2_std_coll_fx_haircut_amt = b2_std_elig_coll_amt;
    else:
        b2_std_coll_fx_haircut_amt = (b2_std_fx_haircut * b2_std_elig_coll_amt);

    return b2_std_coll_fx_haircut_amt;
# end of calculate_b2_std_coll_fx_haircut_amt()


def calculate_blank_crm_amt_std(corep_crm_category, b2_std_eff_gtee_amt, b2_std_eff_cds_amt,
                                b2_std_eff_coll_amt_post_hcut):
    blank_crm_amt_std = 0.0;
    b2_std_eff_gtee_amt_tmp = 0.0;
    b2_std_eff_cds_amt_tmp = 0.0;
    b2_std_eff_coll_amt_post_hcut_tmp = 0.0;

    if (b2_std_eff_gtee_amt is None):
        b2_std_eff_gtee_amt = 0.0;
    b2_std_eff_gtee_amt_tmp = b2_std_eff_gtee_amt;

    if (b2_std_eff_cds_amt is None):
        b2_std_eff_cds_amt = 0.0;
    b2_std_eff_cds_amt_tmp = b2_std_eff_cds_amt;

    if (b2_std_eff_coll_amt_post_hcut is None):
        b2_std_eff_coll_amt_post_hcut = 0.0;
    b2_std_eff_coll_amt_post_hcut_tmp = b2_std_eff_coll_amt_post_hcut;

    if (corep_crm_category is None):
        blank_crm_amt_std = (b2_std_eff_gtee_amt_tmp + b2_std_eff_cds_amt_tmp + b2_std_eff_coll_amt_post_hcut_tmp)
    else:
        blank_crm_amt_std = 0.0;

    return blank_crm_amt_std;
# end of calculate_blank_crm_amt_std()


def calculate_blank_crm_amt_irb(corep_crm_category, b2_irb_gtee_amt, b2_irb_coll_amt):
    blank_crm_amt_irb = 0.0;
    b2_irb_gtee_amt_tmp = 0.0;
    b2_irb_coll_amt_tmp = 0.0;

    if b2_irb_gtee_amt is None:
        b2_irb_gtee_amt = 0.0;
    b2_irb_gtee_amt_tmp = b2_irb_gtee_amt;

    if b2_irb_coll_amt is None:
        b2_irb_coll_amt = 0.0;
    b2_irb_coll_amt_tmp = b2_irb_coll_amt;

    if (corep_crm_category is None):
        blank_crm_amt_irb = (b2_irb_gtee_amt_tmp + b2_irb_coll_amt_tmp)
    else:
        blank_crm_amt_irb = 0.0;

    return blank_crm_amt_irb;
# end of calculate_blank_crm_amt_irb()


def calculate_b2_irb_crm_cod_amt(collateral_type_code, sys_protection_type, sys_code, corep_crm_category,
                                sec_discntd_b2_irb_colltrl_amt, double_default_flag, b2_irb_coll_amt):
    b2_irb_crm_cod_amt = 0.0;

    if (collateral_type_code == 'CO' and sys_protection_type == 'EP' and sys_code == 'RMP'):
        if (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION'):
            if sec_discntd_b2_irb_colltrl_amt is None:
                sec_discntd_b2_irb_colltrl_amt = 0.0;
            elif (sec_discntd_b2_irb_colltrl_amt != 0):
                if not double_default_flag:
                    double_default_flag = 'N'
                elif (double_default_flag != 'y'):
                    b2_irb_crm_cod_amt = sec_discntd_b2_irb_colltrl_amt;
        elif (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION'):
            if sec_discntd_b2_irb_colltrl_amt is None:
                sec_discntd_b2_irb_colltrl_amt = 0.0;
            elif (sec_discntd_b2_irb_colltrl_amt == 0):
                if not double_default_flag:
                    double_default_flag = 'N'
                elif (double_default_flag != 'y'):
                    if not b2_irb_coll_amt:
                        b2_irb_coll_amt = 0.0;
                    b2_irb_crm_cod_amt = b2_irb_coll_amt;
        else:
            b2_irb_crm_cod_amt = 0.0;
    else:
        b2_irb_crm_cod_amt = 0.0;

    return b2_irb_crm_cod_amt;
# end of calculate_b2_irb_crm_cod_amt()


def calculate_b2_irb_crm_lip_amt(collateral_type_code, sys_protection_type, sys_code, corep_crm_category,
                                sec_discntd_b2_irb_colltrl_amt, double_default_flag, b2_irb_coll_amt):
    b2_irb_crm_lip_amt = 0.0;

    if (collateral_type_code == 'LP' and sys_protection_type == 'EP' and sys_code == 'RMP'):
        if (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION'):
            if sec_discntd_b2_irb_colltrl_amt is None:
                sec_discntd_b2_irb_colltrl_amt = 0.0;
            elif (sec_discntd_b2_irb_colltrl_amt != 0):
                if not double_default_flag:
                    double_default_flag = 'N'
                elif (double_default_flag != 'y'):
                    b2_irb_crm_lip_amt = sec_discntd_b2_irb_colltrl_amt;
        elif (corep_crm_category == 'OTHER FUNDED CREDIT PROTECTION'):
            if sec_discntd_b2_irb_colltrl_amt is None:
                sec_discntd_b2_irb_colltrl_amt = 0.0;
            elif (sec_discntd_b2_irb_colltrl_amt == 0):
                if not double_default_flag:
                    double_default_flag = 'N'
                elif (double_default_flag != 'y'):
                    if b2_irb_coll_amt is None:
                        b2_irb_coll_amt = 0.0;
                    b2_irb_crm_lip_amt = b2_irb_coll_amt;
        else:
            b2_irb_crm_lip_amt = 0.0;
    else:
        b2_irb_crm_lip_amt = 0.0;

    return b2_irb_crm_lip_amt;
# end of calculate_b2_irb_crm_lip_amt()


def calculate_qccp_flag(external_counterparty_qccp_flag):
    qccp_flag = "";

    if (external_counterparty_qccp_flag is not None):
        qccp_flag = external_counterparty_qccp_flag;

    return qccp_flag;
# end of calculate_qccp_flag()


def calculate_b2_std_provisions_amt(rep_provision_amt_gbp, b3_cva_provision_amt):
    b2_std_provisions_amt = 0.0;

    if (rep_provision_amt_gbp is None):
        rep_provision_amt_gbp = 0.0;

    if (b3_cva_provision_amt is None):
        b3_cva_provision_amt = 0.0;

    b2_std_provisions_amt = (rep_provision_amt_gbp + b3_cva_provision_amt);

    return b2_std_provisions_amt;
# end of  calculate_b2_std_provisions_amt()


def calculate_b2_std_provisions_amt_ofbal(corep_product_category, rep_provision_amt_gbp, b3_cva_provision_amt):
    b2_std_provisions_amt_ofbal = 0.0;

    if (rep_provision_amt_gbp is None):
        rep_provision_amt_gbp = 0.0;

    if (b3_cva_provision_amt is None):
        b3_cva_provision_amt = 0.0;

    if (corep_product_category == 'OFF BALANCE SHEET'):
        b2_std_provisions_amt_ofbal = (rep_provision_amt_gbp + b3_cva_provision_amt);

    return b2_std_provisions_amt_ofbal;
# end of calculate_b2_std_provisions_amt_ofbal()


def calculate_b2_std_ead_post_crm_amt_ofbal(corep_product_category, retail_off_bal_sheet_flag,
                                            citizens_override_flag, b2_std_ead_post_crm_amt,
                                            b2_app_ead_post_crm_amt, ret_rwa_post_crm_amt_pct):
    b2_std_ead_post_crm_amt_ofbal = 0.0;

    if (b2_std_ead_post_crm_amt is None):
        b2_std_ead_post_crm_amt = 0.0;

    if (b2_app_ead_post_crm_amt is None):
        b2_app_ead_post_crm_amt = 0.0;

    if ((corep_product_category == 'ON BALANCE SHEET') and (retail_off_bal_sheet_flag == 'Y')):
        crm_amt = 0.0;

        if (citizens_override_flag == 'Y'):
            crm_amt = b2_std_ead_post_crm_amt;
        else:
            crm_amt = b2_app_ead_post_crm_amt;

        b2_std_ead_post_crm_amt_ofbal = (crm_amt * (1 - ret_rwa_post_crm_amt_pct));

    elif (corep_product_category == 'OFF BALANCE SHEET' and citizens_override_flag == 'Y'):
        b2_std_ead_post_crm_amt_ofbal = b2_std_ead_post_crm_amt;
    elif (corep_product_category == 'OFF BALANCE SHEET' and citizens_override_flag == 'N'):
        b2_std_ead_post_crm_amt_ofbal = b2_app_ead_post_crm_amt;
    else:
        b2_std_ead_post_crm_amt_ofbal = 0.0;

    return b2_std_ead_post_crm_amt_ofbal
# end of calculate_b2_std_ead_post_crm_amt_ofbal()


def calculate_b2_std_ccf_exp(rwa_approach_code, corep_product_category, retail_off_bal_sheet_flag,
                            ret_rwa_post_crm_amt_pct, citizens_override_flag, b2_std_ead_post_crm_amt,
                            b2_app_ead_post_crm_amt, b2_std_ccf_input, undrawn_commitment_amt, off_balance_exposure):
    b2_std_ccf_exp = 0.0;

    if (b2_std_ead_post_crm_amt is None):
        b2_std_ead_post_crm_amt = 0.0;

    if (b2_app_ead_post_crm_amt is None):
        b2_app_ead_post_crm_amt = 0.0;

    if (b2_std_ccf_input is None):
        b2_std_ccf_input = 0.0;

    if (undrawn_commitment_amt is None):
        undrawn_commitment_amt = 0.0;

    if (off_balance_exposure is None):
        off_balance_exposure = 0.0;

    if (ret_rwa_post_crm_amt_pct is None):
        ret_rwa_post_crm_amt_pct = 0.0;

    if (rwa_approach_code == 'STD'):
        innerlogic1 = 0.0;
        innerlogic2 = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET' and retail_off_bal_sheet_flag == 'Y'):
            innerlogic1 = 1 - ret_rwa_post_crm_amt_pct;
        elif (corep_product_category == 'OFF BALANCE SHEET'):
            innerlogic1 = 1;
        else:
            innerlogic1 = 0.0;

        if (citizens_override_flag == 'Y'):
            innerlogic2 = b2_std_ead_post_crm_amt;
        else:
            innerlogic2 = b2_app_ead_post_crm_amt;

        if ((innerlogic1 * innerlogic2 != 0) and (b2_std_ccf_input == 0)):
            innerlogic3 = 0.0;
            innerlogic4 = 0.0;

            if (corep_product_category == 'ON BALANCE SHEET' and retail_off_bal_sheet_flag == 'Y'):
                innerlogic3 = 1 - ret_rwa_post_crm_amt_pct;
            elif (corep_product_category == 'OFF BALANCE SHEET'):
                innerlogic3 = 1;
            else:
                innerlogic3 = 0.0;

            if (citizens_override_flag == 'Y'):
                innerlogic4 = b2_std_ead_post_crm_amt;
            else:
                innerlogic4 = b2_app_ead_post_crm_amt;

            newamt = 0;

            if ((undrawn_commitment_amt + off_balance_exposure) == 0):
                newamt = 1
            else:
                newamt = undrawn_commitment_amt + off_balance_exposure
                
            if (((innerlogic3 * innerlogic4) / newamt) < 0.35):
                b2_std_ccf_exp = 0.2;
            elif((((innerlogic3 * innerlogic4) / newamt) >= 0.35) and (((innerlogic3 * innerlogic4) / newamt) <= 0.75)):
                b2_std_ccf_exp = 0.5;
            else:
                b2_std_ccf_exp = 1;
        else:
            b2_std_ccf_exp = b2_std_ccf_input;
    else:
        b2_std_ccf_exp = b2_std_ccf_input;

    return b2_std_ccf_exp;
# end of  calculate_b2_std_ccf_exp()


def calculate_std_orig_exp_pre_conv_ofbal(aclm_product_type, bank_base_role_code, grdw_pool_id,
                                        off_bal_led_exp_gbp_post_sec, comm_less_than_1_year,
                                        comm_more_than_1_year, undrawn_commitment_amt_pst_sec,
                                        off_balance_exposure, undrawn_commitment_amt):
    std_orig_exp_pre_conv_ofbal = 0.0;

    if (off_bal_led_exp_gbp_post_sec is None):
        off_bal_led_exp_gbp_post_sec = 0.0;

    if (comm_less_than_1_year is None):
        comm_less_than_1_year = 0.0;

    if (comm_more_than_1_year is None):
        comm_more_than_1_year = 0.0;

    if (off_balance_exposure is None):
        off_balance_exposure = 0.0;

    if (undrawn_commitment_amt is None):
        undrawn_commitment_amt = 0.0;

    if (aclm_product_type is None):
        aclm_product_type = '';

    if (aclm_product_type.upper() in ('UNDRAWN-COMM', 'OFF_BALANCE', 'RETAIL', 'ON_BALANCE') and bank_base_role_code == 'P'):

        if (grdw_pool_id is not None and len(grdw_pool_id) != 0 ):
            std_orig_exp_pre_conv_ofbal = off_bal_led_exp_gbp_post_sec + comm_less_than_1_year + comm_more_than_1_year;
        else:
            std_orig_exp_pre_conv_ofbal = off_bal_led_exp_gbp_post_sec + undrawn_commitment_amt_pst_sec;
    elif (aclm_product_type.upper() in ('UNDRAWN-COMM', 'OFF_BALANCE', 'RETAIL', 'ON_BALANCE')
        and bank_base_role_code != 'P'):

        if (grdw_pool_id is not None and len(grdw_pool_id) != 0 ):
            std_orig_exp_pre_conv_ofbal = (off_balance_exposure + comm_less_than_1_year + comm_more_than_1_year);
        else:
            std_orig_exp_pre_conv_ofbal = off_balance_exposure + undrawn_commitment_amt;

    return std_orig_exp_pre_conv_ofbal;
# end of  calculate_std_orig_exp_pre_conv_ofbal()


def calculate_b2_std_fully_adj_exp_ccf_0(b2_std_ccf_exp, std_orig_exp_pre_conv_ofbal):
    b2_std_fully_adj_exp_ccf_0 = 0.0;

    if (b2_std_ccf_exp is None):
        b2_std_ccf_exp = 0.0;
        
    if (std_orig_exp_pre_conv_ofbal is None):
        std_orig_exp_pre_conv_ofbal = 0.0;

    if (b2_std_ccf_exp == 0):
        b2_std_fully_adj_exp_ccf_0 = std_orig_exp_pre_conv_ofbal;

    return b2_std_fully_adj_exp_ccf_0;
# end of calculate_b2_std_fully_adj_exp_ccf_0()


def calculate_b2_std_ead_post_crm_amt_onbal(corep_product_category, retail_off_bal_sheet_flag,
                                            citizens_override_flag, b2_std_ead_post_crm_amt,
                                            b2_app_ead_post_crm_amt, ret_rwa_post_crm_amt_pct,
                                            aclm_product_type):
    b2_std_ead_post_crm_amt_onbal = 0.0;

    if (b2_std_ead_post_crm_amt is None):
        b2_std_ead_post_crm_amt = 0.0;

    if (b2_app_ead_post_crm_amt is None):
        b2_app_ead_post_crm_amt = 0.0;

    if (ret_rwa_post_crm_amt_pct is None):
        ret_rwa_post_crm_amt_pct = 0.0;

    if (retail_off_bal_sheet_flag is None):
        retail_off_bal_sheet_flag = 'X';

    if (corep_product_category == 'ON BALANCE SHEET' and retail_off_bal_sheet_flag == 'Y'):
        ret_rwa_post_crm_amt_pct_new = 0.0;
        if (citizens_override_flag == 'Y'):
            ret_rwa_post_crm_amt_pct_new = b2_std_ead_post_crm_amt;
        else:
            ret_rwa_post_crm_amt_pct_new = b2_app_ead_post_crm_amt;
        b2_std_ead_post_crm_amt_onbal = (ret_rwa_post_crm_amt_pct_new * ret_rwa_post_crm_amt_pct)
    elif ((corep_product_category == 'ON BALANCE SHEET' or aclm_product_type in ('EQUITY', 'NCA'))
        and retail_off_bal_sheet_flag != 'Y' and citizens_override_flag == 'Y'):

        b2_std_ead_post_crm_amt_onbal = b2_std_ead_post_crm_amt;
    elif ((corep_product_category == 'ON BALANCE SHEET' or aclm_product_type in ('EQUITY', 'NCA'))
        and retail_off_bal_sheet_flag != 'Y' and citizens_override_flag == 'N'):

        b2_std_ead_post_crm_amt_onbal = b2_app_ead_post_crm_amt;

    return b2_std_ead_post_crm_amt_onbal;
# end of calculate_b2_std_ead_post_crm_amt_onbal()


def calculate_b2_std_orig_exp_pre_con_factor_exp(aclm_product_type, b2_std_orig_exp_pre_con_factor,
                                                b3_cva_provision_amt):
    b2_std_orig_exp_pre_con_factor_exp = 0.0;

    if (b2_std_orig_exp_pre_con_factor is None):
        b2_std_orig_exp_pre_con_factor = 0.0;

    if (b3_cva_provision_amt is None):
        b3_cva_provision_amt = 0.0;

    if (aclm_product_type in ('BASEL-OTC', 'REPO')):

        b2_std_orig_exp_pre_con_factor_exp = b2_std_orig_exp_pre_con_factor + b3_cva_provision_amt;
    else:
        b2_std_orig_exp_pre_con_factor_exp = b2_std_orig_exp_pre_con_factor;

    return b2_std_orig_exp_pre_con_factor_exp;
# end of calculate_b2_std_orig_exp_pre_con_factor_exp()


def calculate_std_orig_exp_pre_conv_onbal(corep_product_category, b2_std_orig_exp_pre_con_factor_exp,
                                        std_orig_exp_pre_conv_ofbal):
    std_orig_exp_pre_conv_onbal = 0.0;

    if (b2_std_orig_exp_pre_con_factor_exp is None):
        b2_std_orig_exp_pre_con_factor_exp = 0.0;

    if (std_orig_exp_pre_conv_ofbal is None):
        std_orig_exp_pre_conv_ofbal = 0.0;

    if (corep_product_category == 'ON BALANCE SHEET'):
        std_orig_exp_pre_conv_onbal = b2_std_orig_exp_pre_con_factor_exp - std_orig_exp_pre_conv_ofbal;

    return std_orig_exp_pre_conv_onbal;
# end of calculate_std_orig_exp_pre_conv_onbal()


def calculate_std_orig_exp_pre_conv_onbal(corep_product_category, b2_std_orig_exp_pre_con_factor_exp,
                                        std_orig_exp_pre_conv_ofbal):
    std_orig_exp_pre_conv_onbal = 0.0;

    if (b2_std_orig_exp_pre_con_factor_exp is None):
        b2_std_orig_exp_pre_con_factor_exp = 0.0;

    if (std_orig_exp_pre_conv_ofbal is None):
        std_orig_exp_pre_conv_ofbal = 0.0;

    if (corep_product_category == 'ON BALANCE SHEET'):
        std_orig_exp_pre_conv_onbal = b2_std_orig_exp_pre_con_factor_exp - std_orig_exp_pre_conv_ofbal;

    return std_orig_exp_pre_conv_onbal
# end of calculate_std_orig_exp_pre_conv_onbal()


def calculate_rwa_post_crm_amount(citizens_override_flag, b2_app_rwa_post_crm_amt, b2_std_rwa_post_crm_amt):
    rwa_post_crm_amount = 0.0;

    if (b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;

    if (b2_std_rwa_post_crm_amt is None):
        b2_std_rwa_post_crm_amt = 0.0;

    if (citizens_override_flag == 'Y'):

        rwa_post_crm_amount = b2_std_rwa_post_crm_amt;

    else:
        rwa_post_crm_amount = b2_app_rwa_post_crm_amt;

    return rwa_post_crm_amount;
# end of calculate_rwa_post_crm_amount()


def calculate_b2_std_crm_tot_outflow_fur_exp(b2_std_crm_tot_outflow_agg, ead_post_crm_amt):
    b2_std_crm_tot_outflow_exp = 0.0;

    if (b2_std_crm_tot_outflow_agg is None):
        b2_std_crm_tot_outflow_agg = 0.0;

    if (ead_post_crm_amt is None):
        ead_post_crm_amt = 0.0;

    if (b2_std_crm_tot_outflow_agg > 0):
        newoutflow = 0.0;
        if (ead_post_crm_amt <= 0):
            newoutflow = ead_post_crm_amt;
        else:
            newoutflow = (b2_std_crm_tot_outflow_agg if b2_std_crm_tot_outflow_agg < ead_post_crm_amt else ead_post_crm_amt)

        b2_std_crm_tot_outflow_exp = newoutflow;
    else:
        b2_std_crm_tot_outflow_exp = 0.0;

    return b2_std_crm_tot_outflow_exp;
# end of calculate_b2_std_crm_tot_outflow_fur_exp()


def calculate_b2_std_exposure_value_ofbal_exp(b2_std_ead_post_crm_amt_ofbal, corep_product_category,
                                            data):
    b2_std_exposure_value_ofbal_exp = 0.0;
    
    if(data is None or len(data) == 0):
        return b2_std_exposure_value_ofbal_exp;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    if (b2_std_ead_post_crm_amt_ofbal is None):
        b2_std_ead_post_crm_amt_ofbal = 0.0;

    if ((corep_product_category is not None) and (corep_product_category.upper() == 'OFF BALANCE SHEET')):
        b2_std_exposure_value_ofbal_exp = (b2_std_ead_post_crm_amt_ofbal - b2_std_crm_tot_outflow_exp);
    else:
        b2_std_exposure_value_ofbal_exp = 0.0;
    b2_std_exposure_value_ofbal_exp = (b2_std_ead_post_crm_amt_ofbal - (b2_std_crm_tot_outflow_exp if corep_product_category == 'OFF BALANCE SHEET' else 0.0))

    return b2_std_exposure_value_ofbal_exp;
# end of calculate_b2_std_exposure_value_ofbal_exp()


def calculate_b2_std_fully_adj_exp_ccf_50_exp(b2_std_ccf_exp, b2_std_ead_post_crm_amt_ofbal, corep_product_category,
                                            data, default_fund_contrib_indicator):
    b2_std_fully_adj_exp_ccf_50 = 0.0;

    if (b2_std_ccf_exp is None):
        b2_std_ccf_exp = 0.0;

    if (b2_std_ead_post_crm_amt_ofbal is None):
        b2_std_ead_post_crm_amt_ofbal = 0.0;

    if(data is None or len(data) == 0):
        return b2_std_fully_adj_exp_ccf_50;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    if (b2_std_ccf_exp == 0.5):
        b2_std_crm_tot_outflow_exp_new = 0.0;
        
        if (default_fund_contrib_indicator == 'N'):
            
            if (corep_product_category == 'OFF BALANCE SHEET'):
                b2_std_crm_tot_outflow_exp_new = b2_std_crm_tot_outflow_exp;

            b2_std_fully_adj_exp_ccf_50 = (b2_std_ead_post_crm_amt_ofbal - b2_std_crm_tot_outflow_exp_new) / 0.5;

    return b2_std_fully_adj_exp_ccf_50;
# end of calculate_b2_std_fully_adj_exp_ccf_50_exp()


def calculate_b2_std_fully_adj_exp_ccf_20_exp(b2_std_ccf_exp, b2_std_ead_post_crm_amt_ofbal, corep_product_category, 
                                            data, default_fund_contrib_indicator):
    b2_std_fully_adj_exp_ccf_20_exp = 0.0;

    if (b2_std_ccf_exp is None):
        b2_std_ccf_exp = 0.0;

    if(data is None or len(data) == 0):
        return b2_std_fully_adj_exp_ccf_20_exp;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    if (b2_std_ead_post_crm_amt_ofbal is None):
        b2_std_ead_post_crm_amt_ofbal = 0.0;

    if (b2_std_ccf_exp == 0.2):
        b2_std_ead_post_crm_amt_ofbal_new = 0.0;
        
        if (default_fund_contrib_indicator == 'N'):
            intnewvalue = 0.0;
            
            if (corep_product_category == 'OFF BALANCE SHEET'):
                intnewvalue = b2_std_crm_tot_outflow_exp 
                
            b2_std_ead_post_crm_amt_ofbal_new = (b2_std_ead_post_crm_amt_ofbal - intnewvalue)
            
        b2_std_fully_adj_exp_ccf_20_exp = (b2_std_ead_post_crm_amt_ofbal_new / 0.2);

    return b2_std_fully_adj_exp_ccf_20_exp;
# end of calculate_b2_std_fully_adj_exp_ccf_20_exp()


def calculate_b2_std_exposure_value_exp(ead_post_crm_amt, data):
    b2_std_exposure_value_exp = 0.0;

    if (ead_post_crm_amt is None):
        ead_post_crm_amt = 0.0;

    if(data is None or len(data) == 0):
        b2_std_crm_tot_outflow_exp = 0.0;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    b2_std_exposure_value_exp = (ead_post_crm_amt - b2_std_crm_tot_outflow_exp);

    return b2_std_exposure_value_exp;
# end of calculate_b2_std_exposure_value_exp()


def calculate_b2_std_exposure_value_onbal_expone(b2_std_ead_post_crm_amt_onbal, corep_product_category,
                                                data):
    b2_std_exposure_value_onbal_expone = 0.0;

    if (b2_std_ead_post_crm_amt_onbal is None):
        b2_std_ead_post_crm_amt_onbal = 0.0;

    if(data is None or len(data) == 0):
        return b2_std_exposure_value_onbal_expone;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    subtract_value = 0.0
    
    if (corep_product_category == 'ON BALANCE SHEET'):
        subtract_value = b2_std_crm_tot_outflow_exp
    else:
        subtract_value = 0.0

    b2_std_exposure_value_onbal_expone = (b2_std_ead_post_crm_amt_onbal - (subtract_value));

    return b2_std_exposure_value_onbal_expone;
# end of calculate_b2_std_exposure_value_onbal_expone()


def calculate_b2_std_exposure_value_onbal_exp(crd4_rep_std_flag, retail_off_bal_sheet_flag, corep_product_category,
                                            b2_std_exposure_value, b2_std_exposure_value_ofbal,
                                            b2_std_exposure_value_onbal_expone):
    b2_std_exposure_value_onbal_exp = 0.0;

    if (b2_std_exposure_value is None):
        b2_std_exposure_value = 0.0;

    if (b2_std_exposure_value_ofbal is None):
        b2_std_exposure_value_ofbal = 0.0;

    if (b2_std_exposure_value_onbal_expone is None):
        b2_std_exposure_value_onbal_expone = 0.0;

    if ((crd4_rep_std_flag == 'Y') and (retail_off_bal_sheet_flag == 'Y') and
            (corep_product_category == 'ON BALANCE SHEET')):

        b2_std_exposure_value_onbal_exp = (b2_std_exposure_value - b2_std_exposure_value_ofbal);
    else:
        b2_std_exposure_value_onbal_exp = b2_std_exposure_value_onbal_expone

    return b2_std_exposure_value_onbal_exp;
# end of calculate_b2_std_exposure_value_onbal_exp()


def calculate_b2_std_eff_fin_coll_amt_final(data, reporting_reg_code, deal_id, aclm_product_type,
                                            std_orig_exp_pre_conv_onbal, b2_std_exposure_value_onbal_exp):
    b2_std_eff_fin_coll_amt_final = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_eff_fin_coll_amt_final;
    else:
        b2_std_eff_fin_coll_amt = data[1];
        if (b2_std_eff_fin_coll_amt is None):
            b2_std_eff_fin_coll_amt = 0.0;

    if (std_orig_exp_pre_conv_onbal is None):
        std_orig_exp_pre_conv_onbal = 0.0;

    if (b2_std_exposure_value_onbal_exp is None):
        b2_std_exposure_value_onbal_exp = 0.0;

    if ((reporting_reg_code.upper() == 'NL-DNB') and (deal_id[3:5] == 'WM')
            and (aclm_product_type.upper() == 'ON_BALANCE')):
        b2_std_eff_fin_coll_amt_final = std_orig_exp_pre_conv_onbal - b2_std_exposure_value_onbal_exp;
    else:
        b2_std_eff_fin_coll_amt_final = b2_std_eff_fin_coll_amt;

    return b2_std_eff_fin_coll_amt_final;
# end of calculate_b2_std_eff_fin_coll_amt_final;


def calculate_b2_std_crm_tot_outflow_onbal(corep_product_category, data):
    b2_std_crm_tot_outflow_onbal = 0.0;

    if(data is None or len(data) == 0):
        return b2_std_crm_tot_outflow_onbal;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    if (corep_product_category == 'ON BALANCE SHEET'):
        b2_std_crm_tot_outflow_onbal = b2_std_crm_tot_outflow_exp

    return b2_std_crm_tot_outflow_onbal;
# end of calculate_b2_std_crm_tot_outflow_onbal()


def calculate_b2_std_crm_tot_outflow_ofbal(corep_product_category, b2_std_ccf_exp, data):
    b2_std_crm_tot_outflow_ofbal = 0.0;

    if (b2_std_ccf_exp is None):
        b2_std_ccf_exp = 0.0

    if(data is None or len(data) == 0):
        return b2_std_crm_tot_outflow_ofbal;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    val = 0.0;
    b2_std_ccf2 = 0.0

    if (corep_product_category == 'OFF BALANCE SHEET'):        
        if (b2_std_ccf_exp == 0.2):
            val = b2_std_crm_tot_outflow_exp / b2_std_ccf_exp;
        elif (b2_std_ccf_exp == 0.5):
            val = b2_std_crm_tot_outflow_exp / b2_std_ccf_exp;
        elif (b2_std_ccf_exp == 1):
            val = b2_std_crm_tot_outflow_exp / b2_std_ccf_exp;
        else:
            val = 0.0;

    b2_std_crm_tot_outflow_ofbal = (b2_std_ccf2 + val);

    return b2_std_crm_tot_outflow_ofbal;
# end of calculate_b2_std_crm_tot_outflow_ofbal()


def calculate_b2_std_rwa_amt_expone(rwa_post_crm_amount, data):
    b2_std_rwa_amt_expone = 0.0;

    if (data is None or len(data) == 0):
        b2_std_gtee_cds_rwa = 0.0;
    else:
        b2_std_gtee_cds_rwa = data[2];
        if (b2_std_gtee_cds_rwa is None):
            b2_std_gtee_cds_rwa = 0.0;

    if (rwa_post_crm_amount is None):
        rwa_post_crm_amount = 0.0;

    if (rwa_post_crm_amount <= 0):
        b2_std_rwa_amt_expone = rwa_post_crm_amount;
    elif (rwa_post_crm_amount > 0):
        if (b2_std_gtee_cds_rwa >= rwa_post_crm_amount):
            b2_std_rwa_amt_expone = (b2_std_gtee_cds_rwa / b2_std_gtee_cds_rwa) * rwa_post_crm_amount;
        elif (b2_std_gtee_cds_rwa < rwa_post_crm_amount):
            b2_std_rwa_amt_expone = b2_std_gtee_cds_rwa;

    return b2_std_rwa_amt_expone;
# end of calculate_b2_std_rwa_amt_expone()


def calculate_b2_std_rwa_amt_stg_three(b2_app_rwa_post_crm_amt, data):
    b2_std_rwa_amt_stg_three = 0.0;

    if (data is None or len(data) == 0):
        b2_std_crm_tot_outflow_rwa = 0.0;
    else:
        b2_std_crm_tot_outflow_rwa = data[3];
        if (b2_std_crm_tot_outflow_rwa is None):
            b2_std_crm_tot_outflow_rwa = 0.0;

    if (b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;

    b2_std_rwa_amt_stg_three = (b2_app_rwa_post_crm_amt - b2_std_crm_tot_outflow_rwa);

    return b2_std_rwa_amt_stg_three
# end of calculate_b2_std_rwa_amt_stg_three()


def calculate_b2_std_rwa_pre_sprt_amt_exp(crr2_501a_discount_flag, b2_std_rwa_amt_stg_three,
                                        sme_discount_factor, v_infra_discount_factor, b3_sme_discount_flag):
    b2_std_rwa_pre_sprt_amt_exp = 0.0;
    b2_std_rwa_pre_sprt_amt_new = 0.0;

    if (sme_discount_factor is None):
        sme_discount_factor = 0.0;

    if (b2_std_rwa_amt_stg_three is None):
        b2_std_rwa_amt_stg_three = 0.0;

    if (crr2_501a_discount_flag == 'Y'):
        b2_std_rwa_pre_sprt_amt_new = 0.0;

        if ((v_infra_discount_factor is None) or (v_infra_discount_factor == 0)):
            b2_std_rwa_pre_sprt_amt_new = 1.0;
        else:
            b2_std_rwa_pre_sprt_amt_new = float(str(v_infra_discount_factor).strip());

        if ((b2_std_rwa_pre_sprt_amt_new > 0.0) or (b2_std_rwa_pre_sprt_amt_new < 0.0)):
            b2_std_rwa_pre_sprt_amt_exp = b2_std_rwa_amt_stg_three / b2_std_rwa_pre_sprt_amt_new;
    elif (b3_sme_discount_flag == 'Y'):
        b2_std_rwa_pre_sprt_amt_new = 0.0;

        if (sme_discount_factor is None or sme_discount_factor == 0):
            b2_std_rwa_pre_sprt_amt_new = 1.0;
        else:
            b2_std_rwa_pre_sprt_amt_new = sme_discount_factor;

        if ((b2_std_rwa_pre_sprt_amt_new > 0.0) or (b2_std_rwa_pre_sprt_amt_new < 0.0)):
            b2_std_rwa_pre_sprt_amt_exp = b2_std_rwa_amt_stg_three / b2_std_rwa_pre_sprt_amt_new;
    else:
        b2_std_rwa_pre_sprt_amt_exp = b2_std_rwa_amt_stg_three;

    return b2_std_rwa_pre_sprt_amt_exp;
# end of calculate_b2_std_rwa_pre_sprt_amt_exp()


def calculate_b2_std_rwa_amt_onbal_exp(b2_std_rwa_post_crm_amt_onbal, corep_product_category, data):
    b2_std_rwa_amt_onbal = 0.0;
    
    if (data is None or len(data) == 0):
        b2_std_crm_tot_outflow_rwa = 0.0;
    else:
        b2_std_crm_tot_outflow_rwa = data[3];
        if (b2_std_crm_tot_outflow_rwa is None):
            b2_std_crm_tot_outflow_rwa = 0.0

    if (b2_std_rwa_post_crm_amt_onbal is None):
        b2_std_rwa_post_crm_amt_onbal = 0.0

    if ((corep_product_category is not None) and (corep_product_category.upper() == 'ON BALANCE SHEET')):
        temp_val = b2_std_crm_tot_outflow_rwa;
    else:
        temp_val = 0.0;

    b2_std_rwa_amt_onbal = (b2_std_rwa_post_crm_amt_onbal - temp_val);

    return b2_std_rwa_amt_onbal;
# end of calculate_b2_std_rwa_amt_onbal_exp()


def calculate_b2_std_rwa_amt_exp(b2_app_rwa_post_crm_amt, data):
    b2_std_rwa_amt = 0.0;

    if (data is None or len(data) == 0):
        b2_std_crm_tot_outflow_rwa = 0.0;
    else:
        b2_std_crm_tot_outflow_rwa = data[3];
        if (b2_std_crm_tot_outflow_rwa is None):
            b2_std_crm_tot_outflow_rwa = 0.0;

    if (b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;

    b2_std_rwa_amt = (b2_app_rwa_post_crm_amt - b2_std_crm_tot_outflow_rwa);

    return b2_std_rwa_amt;
# end of calculate_b2_std_rwa_amt_exp()


def calculate_b2_std_rwa_post_crm_amt_ofbal(corep_product_category, retail_off_bal_sheet_flag,
                                            citizens_override_flag, b2_std_rwa_post_crm_amt,
                                            b2_app_rwa_post_crm_amt, ret_rwa_post_crm_amt_pct):
    b2_std_rwa_post_crm_amt_ofbal = 0.0;

    if (b2_std_rwa_post_crm_amt is None):
        b2_std_rwa_post_crm_amt = 0.0;

    if (b2_app_rwa_post_crm_amt is None):
        b2_app_rwa_post_crm_amt = 0.0;

    if (ret_rwa_post_crm_amt_pct is None):
        ret_rwa_post_crm_amt_pct = 0.0;

    if (corep_product_category == 'ON BALANCE SHEET' and retail_off_bal_sheet_flag == 'Y'):
        b2_std_rwa_post_crm_amt_flag_value = 0.0;
        if (citizens_override_flag == 'Y'):
            b2_std_rwa_post_crm_amt_flag_value = b2_std_rwa_post_crm_amt;
        else:
            b2_std_rwa_post_crm_amt_flag_value = b2_app_rwa_post_crm_amt;
        b2_std_rwa_post_crm_amt_ofbal = b2_std_rwa_post_crm_amt_flag_value * (1 - ret_rwa_post_crm_amt_pct);
    elif (corep_product_category == 'OFF BALANCE SHEET' and citizens_override_flag == 'Y'):
        b2_std_rwa_post_crm_amt_ofbal = b2_std_rwa_post_crm_amt;
    elif (corep_product_category == 'OFF BALANCE SHEET' and citizens_override_flag == 'N'):
        b2_std_rwa_post_crm_amt_ofbal = b2_app_rwa_post_crm_amt;

    return b2_std_rwa_post_crm_amt_ofbal;
# end of calculate_b2_std_rwa_post_crm_amt_ofbal;


def calculate_b2_std_rwa_amt_ofbal_fur_exp(b2_std_rwa_post_crm_amt_ofbal,corep_product_category,
                                        data):
    b2_std_rwa_amt_ofbal_fur_exp = 0.0;
    if (data is None or len(data) == 0):
        b2_std_crm_tot_outflow_rwa = 0.0
    else:
        b2_std_crm_tot_outflow_rwa = data[3]

    if (b2_std_rwa_post_crm_amt_ofbal is None):
        b2_std_rwa_post_crm_amt_ofbal = 0.0;

    if (b2_std_crm_tot_outflow_rwa is None):
        b2_std_crm_tot_outflow_rwa = 0.0;

    finalamount = 0.0;
    
    if (corep_product_category == 'OFF BALANCE SHEET'):
        finalamount = b2_std_crm_tot_outflow_rwa;

    b2_std_rwa_amt_ofbal_fur_exp = (b2_std_rwa_post_crm_amt_ofbal - finalamount);

    return b2_std_rwa_amt_ofbal_fur_exp;
# end of calculate_b2_std_rwa_amt_ofbal_fur_exp


def calculate_b2_std_rwa_amt_onbal_final(crd4_rep_std_flag, retail_off_bal_sheet_flag,
                                        corep_product_category, b2_std_rwa_amt,
                                        b2_std_rwa_amt_ofbal_fur_exp,b2_std_rwa_amt_onbal_fur_exp):
    b2_std_rwa_amt_onbal_final = 0.0;
    
    if (b2_std_rwa_amt is None):
        b2_std_rwa_amt = 0.0;

    if (b2_std_rwa_amt_ofbal_fur_exp is None):
        b2_std_rwa_amt_ofbal_fur_exp = 0.0;

    if (b2_std_rwa_amt_onbal_fur_exp is None):
        b2_std_rwa_amt_onbal_fur_exp = 0.0;

    if ((crd4_rep_std_flag == 'Y' and retail_off_bal_sheet_flag == 'Y')
            and (corep_product_category == 'ON BALANCE SHEET')):
        b2_std_rwa_amt_onbal_final = b2_std_rwa_amt - b2_std_rwa_amt_ofbal_fur_exp;
    else:
        b2_std_rwa_amt_onbal_final = b2_std_rwa_amt_onbal_fur_exp;

    return b2_std_rwa_amt_onbal_final;
# end of calculate_b2_std_rwa_amt_onbal_final()


def calculate_b2_std_rwa_pre_sprt_amt_onbal_exp(crr2_501a_discount_flag,b2_std_rwa_amt_onbal_final,
                                                b3_sme_discount_flag,sme_discount_factor,
                                                v_infra_discount_factor):
    b2_std_rwa_pre_sprt_amt_onbal_exp = 0.0;

    if (b2_std_rwa_amt_onbal_final is None):
        b2_std_rwa_amt_onbal_final = 0.0;

    if (sme_discount_factor is None):
        sme_discount_factor = 0.0;

    if (crr2_501a_discount_flag == 'Y'):
        b2_std_rwa_pre_sprt_amt_onbal_new = 0.0;

        if ((v_infra_discount_factor is None) or (v_infra_discount_factor == 0)):
            b2_std_rwa_pre_sprt_amt_onbal_new = 1
        else:
            b2_std_rwa_pre_sprt_amt_onbal_new = float(v_infra_discount_factor);

        if ((b2_std_rwa_pre_sprt_amt_onbal_new is not None) or (b2_std_rwa_pre_sprt_amt_onbal_new != 0.0)):
            b2_std_rwa_pre_sprt_amt_onbal_exp = (b2_std_rwa_amt_onbal_final / b2_std_rwa_pre_sprt_amt_onbal_new)
        else:
            b2_std_rwa_pre_sprt_amt_onbal_exp = 0.0;
    elif (b3_sme_discount_flag == 'Y'):
        b2_std_rwa_pre_sprt_amt_onbal_new = 0.0;

        if ((sme_discount_factor is None) or (sme_discount_factor == 0)):
            b2_std_rwa_pre_sprt_amt_onbal_new = 1;
        else:
            b2_std_rwa_pre_sprt_amt_onbal_new = float(sme_discount_factor);

        if ((b2_std_rwa_pre_sprt_amt_onbal_new is not None) or (b2_std_rwa_pre_sprt_amt_onbal_new != 0.0)):
            b2_std_rwa_pre_sprt_amt_onbal_exp = (b2_std_rwa_amt_onbal_final / b2_std_rwa_pre_sprt_amt_onbal_new);
        else:
            b2_std_rwa_pre_sprt_amt_onbal_exp = 0.0;
    else:
        b2_std_rwa_pre_sprt_amt_onbal_exp = b2_std_rwa_amt_onbal_final;

    return b2_std_rwa_pre_sprt_amt_onbal_exp;
# end of calculate_b2_std_rwa_pre_sprt_amt_onbal_exp()


def calculate_b2_std_rwa_pre_sprt_amt_ofbal_exp(crr2_501a_discount_flag,b2_std_rwa_amt_ofbal_fur_exp,
                                                b3_sme_discount_flag,sme_discount_factor,
                                                v_infra_discount_factor):
    b2_std_rwa_pre_sprt_amt_ofbal_exp = 0.0;

    if (b2_std_rwa_amt_ofbal_fur_exp is None):
        b2_std_rwa_amt_ofbal_fur_exp = 0.0;

    if (sme_discount_factor is None):
        sme_discount_factor = 0.0;

    if (crr2_501a_discount_flag == 'Y'):
        b2_std_rwa_pre_sprt_amt_ofbal_exp_new = 0.0;

        if ((v_infra_discount_factor is None) or (v_infra_discount_factor == 0)):
            b2_std_rwa_pre_sprt_amt_ofbal_exp_new = 1.0
        else:
            b2_std_rwa_pre_sprt_amt_ofbal_exp_new = float(v_infra_discount_factor);

        if ((b2_std_rwa_pre_sprt_amt_ofbal_exp_new > 0) or (b2_std_rwa_pre_sprt_amt_ofbal_exp_new < 0)):
            b2_std_rwa_pre_sprt_amt_ofbal_exp = (b2_std_rwa_amt_ofbal_fur_exp / b2_std_rwa_pre_sprt_amt_ofbal_exp_new);
        else:
            b2_std_rwa_pre_sprt_amt_ofbal_exp = 0.0;
    elif (b3_sme_discount_flag == 'Y'):
        b2_std_rwa_pre_sprt_amt_ofbal_exp_new = 0.0;

        if (sme_discount_factor is None or sme_discount_factor == 0):
            b2_std_rwa_pre_sprt_amt_ofbal_exp_new = 1.0;
        else:
            b2_std_rwa_pre_sprt_amt_ofbal_exp_new = sme_discount_factor;

        if ((b2_std_rwa_pre_sprt_amt_ofbal_exp_new > 0) or (b2_std_rwa_pre_sprt_amt_ofbal_exp_new < 0)):
            b2_std_rwa_pre_sprt_amt_ofbal_exp = (b2_std_rwa_amt_ofbal_fur_exp / b2_std_rwa_pre_sprt_amt_ofbal_exp_new);
        else:
            b2_std_rwa_pre_sprt_amt_ofbal_exp = 0.0;
    else:
        b2_std_rwa_pre_sprt_amt_ofbal_exp = b2_std_rwa_amt_ofbal_fur_exp;

    return b2_std_rwa_pre_sprt_amt_ofbal_exp;
# end of  calculate_b2_std_rwa_pre_sprt_amt_ofbal_exp()


def calculate_std_act_risk_weight_exp(b2_std_rwa_amt_stg_three, b2_std_exposure_value,rwa_approach_code, crr2_501a_discount_flag,
                                    b3_sme_discount_flag, sme_discount_factor,v_infra_discount_factor, 
                                    std_risk_weight_ratio):
    std_act_risk_weight_exp = 0.0;

    if (sme_discount_factor is None):
        sme_discount_factor = 0.0
        
    if (b2_std_exposure_value is None):
        b2_std_exposure_value = 0.0;

    if (b2_std_rwa_amt_stg_three is None):
        b2_std_rwa_amt_stg_three = 0.0;

    if (std_risk_weight_ratio is None or std_risk_weight_ratio == ''):
        std_risk_weight_ratio = 0.0;

    std_act_risk_weight_exp_final = 0.0;
    b2_std_rwa_amt_final = 0.0;

    if (b2_std_rwa_amt_stg_three == 0.0 and b2_std_exposure_value == 0.0):
        std_act_risk_weight_exp_final = std_risk_weight_ratio
    elif (b2_std_rwa_amt_stg_three != 0.0 and b2_std_exposure_value == 0.0):
        std_act_risk_weight_exp_final = 9999.0
    elif (b2_std_rwa_amt_stg_three / b2_std_exposure_value > 9999):
        std_act_risk_weight_exp_final = 9999.0
    elif (b2_std_rwa_amt_stg_three < 0 and b2_std_exposure_value >= 0):
        std_act_risk_weight_exp_final = 9999.0
    elif (b2_std_rwa_amt_stg_three > 0 and b2_std_exposure_value < 0):
        std_act_risk_weight_exp_final = 9999.0
    else:
        std_act_risk_weight_exp_new = 0.0;
        if (rwa_approach_code == 'STD' and crr2_501a_discount_flag == 'Y'):
            if (v_infra_discount_factor is None or v_infra_discount_factor == 0):
                std_act_risk_weight_exp_new = 1.0;
            else:
                std_act_risk_weight_exp_new = float(v_infra_discount_factor);

            if (std_act_risk_weight_exp_new > 0 or std_act_risk_weight_exp_new < 0):
                b2_std_rwa_amt_final = b2_std_rwa_amt_stg_three / std_act_risk_weight_exp_new;
            else:
                b2_std_rwa_amt_final = 0.0;
        elif (rwa_approach_code == 'STD' and b3_sme_discount_flag == 'Y'):
            
            if (sme_discount_factor is None or sme_discount_factor == 0):
                std_act_risk_weight_exp_new = 1.0;
            else:
                std_act_risk_weight_exp_new = sme_discount_factor;

            if (std_act_risk_weight_exp_new > 0 or std_act_risk_weight_exp_new < 0):
                b2_std_rwa_amt_final = b2_std_rwa_amt_stg_three / std_act_risk_weight_exp_new;
            else:
                b2_std_rwa_amt_final = 0.0;
        else:
            b2_std_rwa_amt_final = b2_std_rwa_amt_stg_three;

        if (b2_std_exposure_value is not None):
            b2_std_rwa_amt_final_final = b2_std_rwa_amt_final / b2_std_exposure_value
        else:
            b2_std_rwa_amt_final_final = 0.0;

        if (b2_std_rwa_amt_final_final < 9999 and b2_std_rwa_amt_final_final is not None):
            std_act_risk_weight_exp_final = b2_std_rwa_amt_final_final
        else:
            std_act_risk_weight_exp_final = 9999.0;

    if (std_act_risk_weight_exp_final is not None):
        std_act_risk_weight_exp = float(std_act_risk_weight_exp_final)
    else:
        std_act_risk_weight_exp = 0.0;

    return std_act_risk_weight_exp;
# end of calculate_std_act_risk_weight_exp()


def calculate_b2_std_crm_tot_outflow_final(corep_product_category,b2_std_crm_tot_outflow_onbal,
                                        data,b2_std_crm_tot_outflow_ofbal):
    b2_std_crm_tot_outflow_final = 0.0;

    if (b2_std_crm_tot_outflow_onbal is None):
        b2_std_crm_tot_outflow_onbal = 0.0;

    if (b2_std_crm_tot_outflow_ofbal is None):
        b2_std_crm_tot_outflow_ofbal = 0.0;

    if(data is None or len(data) == 0):
        return b2_std_crm_tot_outflow_final;
    else:
        b2_std_crm_tot_outflow_exp = data[0];
        
        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    if (corep_product_category == 'ON BALANCE SHEET'):
        b2_std_crm_tot_outflow_final = b2_std_crm_tot_outflow_onbal;
    elif (corep_product_category == 'OFF BALANCE SHEET'):
        b2_std_crm_tot_outflow_final = b2_std_crm_tot_outflow_ofbal;
    else:
        b2_std_crm_tot_outflow_final = b2_std_crm_tot_outflow_exp;

    return b2_std_crm_tot_outflow_final;
# end of calculate_b2_std_crm_tot_outflow_final


def calculate_b2_std_eff_cds_amt_final(corep_product_category,b2_std_crm_tot_outflow_onbal,
                                    b2_std_crm_tot_outflow_ofbal,data):
    b2_std_eff_cds_amt_final = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_eff_cds_amt_final;
    else:
        b2_std_eff_gtee_amt_agg = data[4];

        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;
        
        if (b2_std_eff_gtee_amt_agg is None):
            b2_std_eff_gtee_amt_agg = 0.0;
            
        b2_std_eff_cds_amt_agg = data[5];
        
        if (b2_std_eff_cds_amt_agg is None):
            b2_std_eff_cds_amt_agg = 0.0;

        if (b2_std_crm_tot_outflow_onbal is None):
            b2_std_crm_tot_outflow_onbal = 0.0;

        if (b2_std_crm_tot_outflow_ofbal is None):
            b2_std_crm_tot_outflow_ofbal = 0.0;

        tot_value = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET'):
            tot_value = b2_std_crm_tot_outflow_onbal
        elif (corep_product_category == 'OFF BALANCE SHEET'):
            tot_value = b2_std_crm_tot_outflow_ofbal
        else:
            tot_value = b2_std_crm_tot_outflow_exp

        if ((b2_std_eff_cds_amt_agg + b2_std_eff_gtee_amt_agg) < 0 or (
            b2_std_eff_cds_amt_agg + b2_std_eff_gtee_amt_agg) > 0):
            b2_std_eff_cds_amt_final = b2_std_eff_cds_amt_agg * tot_value / (
                    b2_std_eff_cds_amt_agg + b2_std_eff_gtee_amt_agg)

        return b2_std_eff_cds_amt_final;

    return b2_std_eff_cds_amt_final;
# end of calculate_b2_std_eff_cds_amt_final


def calculate_b2_std_eff_gtee_amt_final(corep_product_category,b2_std_crm_tot_outflow_onbal,
                                        b2_std_crm_tot_outflow_ofbal,data):
    b2_std_eff_gtee_amt_final = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_eff_gtee_amt_final;
    else:
        b2_std_eff_gtee_amt_agg = data[4];
        
        if (b2_std_eff_gtee_amt_agg is None):
            b2_std_eff_gtee_amt_agg = 0.0;

        b2_std_eff_cds_amt_agg = data[5];
        
        if (b2_std_eff_cds_amt_agg is None):
            b2_std_eff_cds_amt_agg = 0.0;

        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

        if (b2_std_crm_tot_outflow_onbal is None):
            b2_std_crm_tot_outflow_onbal = 0.0;

        if (b2_std_crm_tot_outflow_ofbal is None):
            b2_std_crm_tot_outflow_ofbal = 0.0;

        tot_value = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET'):
            tot_value = b2_std_crm_tot_outflow_onbal
        elif (corep_product_category == 'OFF BALANCE SHEET'):
            tot_value = b2_std_crm_tot_outflow_ofbal
        else:
            tot_value = b2_std_crm_tot_outflow_exp

        if ((b2_std_eff_cds_amt_agg + b2_std_eff_gtee_amt_agg) < 0 or (b2_std_eff_cds_amt_agg + b2_std_eff_gtee_amt_agg) > 0):
            b2_std_eff_gtee_amt_final = b2_std_eff_gtee_amt_agg * tot_value/(b2_std_eff_cds_amt_agg + b2_std_eff_gtee_amt_agg)
        
        return b2_std_eff_gtee_amt_final;

    return b2_std_eff_gtee_amt_final;
# end of calculate_b2_std_eff_gtee_amt_final


def calculate_b2_std_fully_adj_exp_ccf_100_exp(b2_std_ccf_exp,default_fund_contrib_indicator,
                                            b2_std_ead_post_crm_amt_ofbal,corep_product_category,
                                            data):
    b2_std_fully_adj_exp_ccf_100_exp = 0.0;

    if (b2_std_ead_post_crm_amt_ofbal is None):
        b2_std_ead_post_crm_amt_ofbal = 0.0;

    if(data is None or len(data) == 0):
        return b2_std_fully_adj_exp_ccf_100_exp;
    else:
        b2_std_crm_tot_outflow_exp = data[0];

        if (b2_std_crm_tot_outflow_exp is None):
            b2_std_crm_tot_outflow_exp = 0.0;

    if (b2_std_ccf_exp == 1):
        if (default_fund_contrib_indicator == 'N'):
            int_amt = 0.0;
            if (corep_product_category == 'OFF BALANCE SHEET'):
                int_amt = b2_std_crm_tot_outflow_exp;

            b2_std_fully_adj_exp_ccf_100_exp = (b2_std_ead_post_crm_amt_ofbal - int_amt) / 1;

    return b2_std_fully_adj_exp_ccf_100_exp;
# end of calculate_b2_std_fully_adj_exp_ccf_100_exp()


def calculate_b2_std_exposure_value_ofbal_subexp(b2_std_eff_gtee_amt, b2_std_eff_cds_amt,b2_std_crm_tot_outflow,data):
    b2_std_exposure_value_ofbal_subexp = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_exposure_value_ofbal_subexp;
    else:
        ead_post_crm_amt1 = data[0];
        corep_product_category = data[1];

        if (ead_post_crm_amt1 is None):
            ead_post_crm_amt1 = 0.0;

        if (b2_std_eff_gtee_amt is None):
            b2_std_eff_gtee_amt = 0.0;

        if (b2_std_eff_cds_amt is None):
            b2_std_eff_cds_amt = 0.0;

        if (b2_std_crm_tot_outflow is None):
            b2_std_crm_tot_outflow = 0.0;

        if (corep_product_category == 'OFF BALANCE SHEET'):
            ead_post_crm_amt_new = 0.0;

            if (ead_post_crm_amt1 <= 0):
                ead_post_crm_amt_new = ead_post_crm_amt1;
            elif (ead_post_crm_amt1 > 0):
                if ((b2_std_eff_gtee_amt + b2_std_eff_cds_amt) >= ead_post_crm_amt1):
                    ead_post_crm_amt_new = (b2_std_eff_gtee_amt + b2_std_eff_cds_amt) / (b2_std_eff_gtee_amt + b2_std_eff_cds_amt) * ead_post_crm_amt1;
                elif (b2_std_crm_tot_outflow < ead_post_crm_amt1):
                    ead_post_crm_amt_new = (b2_std_eff_gtee_amt + b2_std_eff_cds_amt);

            b2_std_exposure_value_ofbal_subexp = ead_post_crm_amt_new;
        return b2_std_exposure_value_ofbal_subexp;

    return b2_std_exposure_value_ofbal_subexp;
# end of  calculate_b2_std_exposure_value_ofbal_subexp()


def calculate_b2_std_fully_adj_exp_ccf_50_subexp(data, b2_std_exposure_value_ofbal):
    b2_std_fully_adj_exp_ccf_50 = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_fully_adj_exp_ccf_50;
    else:
        b2_std_ccf = data[2];

        if (b2_std_ccf is None):
            b2_std_ccf = 0.0;

        if (b2_std_exposure_value_ofbal is None):
            b2_std_exposure_value_ofbal = 0.0;

        if (b2_std_ccf == 0.5):
            b2_std_fully_adj_exp_ccf_50 = b2_std_exposure_value_ofbal / b2_std_ccf;

        return b2_std_fully_adj_exp_ccf_50;

    return b2_std_fully_adj_exp_ccf_50;
# end of calculate_b2_std_fully_adj_exp_ccf_50_subexp


def calculate_b2_std_fully_adj_exp_ccf_20_subexp(data, b2_std_exposure_value_ofbal):
    b2_std_fully_adj_exp_ccf_20 = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_fully_adj_exp_ccf_20;
    else:
        b2_std_ccf = data[2];
        if (b2_std_ccf is None):
            b2_std_ccf = 0.0;

        if (b2_std_exposure_value_ofbal is None):
            b2_std_exposure_value_ofbal = 0.0;

        if (b2_std_ccf == 0.2):
            b2_std_fully_adj_exp_ccf_20 = (b2_std_exposure_value_ofbal / b2_std_ccf);

        return b2_std_fully_adj_exp_ccf_20;

    return b2_std_fully_adj_exp_ccf_20;
# end of calculate_b2_std_fully_adj_exp_ccf_20_subexp()



def calculate_b2_std_exposure_value_subexp(data, b2_std_crm_tot_outflow, b2_std_eff_gtee_amt_int_agg,
                                           b2_std_eff_cds_amt_int_agg, b2_std_eff_gtee_amt_input, b2_std_eff_cds_amt_input):

    b2_std_exposure_value_subexp = 0.0;

    if (data is None or len(data) == 0):
        ead_post_crm_amt = 0.0;
    else:
        ead_post_crm_amt = data[0];
        
        if (ead_post_crm_amt is None):
            ead_post_crm_amt = 0.0;
        
    if (b2_std_crm_tot_outflow is None):
        b2_std_crm_tot_outflow = 0.0;

    if (b2_std_eff_gtee_amt_int_agg is None):
        b2_std_eff_gtee_amt_int_agg = 0.0;


    if (b2_std_eff_cds_amt_int_agg is None):
        b2_std_eff_cds_amt_int_agg = 0.0;

    if(b2_std_eff_gtee_amt_input is None):
        b2_std_eff_gtee_amt_input = 0.0;

    if(b2_std_eff_cds_amt_input is None):
        b2_std_eff_cds_amt_input = 0.0;

    newval = 0.0;
    input_total = b2_std_eff_gtee_amt_input + b2_std_eff_cds_amt_input;

    if (ead_post_crm_amt <= 0):
        b2_std_exposure_value_subexp = ead_post_crm_amt;
    elif (ead_post_crm_amt > 0):
        cond_1_value = (b2_std_eff_gtee_amt_int_agg + b2_std_eff_cds_amt_int_agg);
        if (cond_1_value >= ead_post_crm_amt):
            if (cond_1_value > 0 or cond_1_value < 0):
                newval = input_total / cond_1_value * ead_post_crm_amt;
        elif (b2_std_crm_tot_outflow < ead_post_crm_amt):
            newval = input_total;

        b2_std_exposure_value_subexp = newval;     

    return b2_std_exposure_value_subexp;
# end of calculate_b2_std_exposure_value_subexp()


def calculate_b2_std_exposure_value_onbal_subexp(data,b2_std_crm_tot_outflow,b2_std_eff_gtee_amt, b2_std_eff_cds_amt):
    b2_std_exposure_value_onbal_subexp = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_exposure_value_onbal_subexp;
    else:
        
        ead_post_crm_amt1 = data[0];
        corep_product_category = data[1];

        if (ead_post_crm_amt1 is None):
            ead_post_crm_amt1 = 0.0;

        if (b2_std_eff_gtee_amt is None):
            b2_std_eff_gtee_amt = 0.0;

        if (b2_std_eff_cds_amt is None):
            b2_std_eff_cds_amt = 0.0;

        if (b2_std_crm_tot_outflow is None):
            b2_std_crm_tot_outflow = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET'):
            b2_std_exposure_value_onbal_subexp_new = 0.0;

            if (ead_post_crm_amt1 <= 0):
                b2_std_exposure_value_onbal_subexp_new = ead_post_crm_amt1
            elif (ead_post_crm_amt1 > 0):
                if ((b2_std_eff_gtee_amt + b2_std_eff_cds_amt) >= ead_post_crm_amt1):
                    if ((b2_std_eff_gtee_amt + b2_std_eff_cds_amt) > 0 or (b2_std_eff_gtee_amt + b2_std_eff_cds_amt) < 0):
                        b2_std_exposure_value_onbal_subexp_new = (b2_std_eff_gtee_amt + b2_std_eff_cds_amt) / (
                                b2_std_eff_gtee_amt + b2_std_eff_cds_amt) * ead_post_crm_amt1;
                elif (b2_std_crm_tot_outflow < ead_post_crm_amt1):
                    b2_std_exposure_value_onbal_subexp_new = b2_std_eff_gtee_amt + b2_std_eff_cds_amt;

            b2_std_exposure_value_onbal_subexp = b2_std_exposure_value_onbal_subexp_new;
        return b2_std_exposure_value_onbal_subexp;

    return b2_std_exposure_value_onbal_subexp;
# end of calculate_b2_std_exposure_value_onbal_subexp()


def calculate_b2_std_crm_tot_inflow(data,b2_std_exposure_value_onbal,b2_std_exposure_value_ofbal,
                                    b2_std_exposure_value):
    b2_std_crm_tot_inflow = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_crm_tot_inflow;
    else:
        corep_product_category = data[1];
        b2_std_ccf = data[2]       

        if (b2_std_exposure_value is None):
            b2_std_exposure_value = 0.0;

        if (b2_std_ccf is None):
            b2_std_ccf = 0.0;

        if (b2_std_exposure_value_ofbal is None):
            b2_std_exposure_value_ofbal = 0.0;

        if (b2_std_exposure_value_onbal is None):
            b2_std_exposure_value_onbal = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET'):
            b2_std_crm_tot_inflow = b2_std_exposure_value_onbal;
        elif (corep_product_category == 'OFF BALANCE SHEET'):
            newval = 0.0;
            b2_std_ccf_exp = 0.0;

            if ((b2_std_ccf == 0.2) or (b2_std_ccf == 0.5) or (b2_std_ccf == 1)):
                newval = (b2_std_exposure_value_ofbal / b2_std_ccf)

            b2_std_crm_tot_inflow = (b2_std_ccf_exp + newval)
        else:
            b2_std_crm_tot_inflow = b2_std_exposure_value
        return b2_std_crm_tot_inflow;

    return b2_std_crm_tot_inflow;
# end of calculate_b2_std_crm_tot_inflow()


def calculate_b2_std_crm_tot_inflow_onbal(data, b2_std_exposure_value_onbal):
    b2_std_crm_tot_inflow_onbal = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_crm_tot_inflow_onbal;
    else:
        corep_product_category = data[1];

        if (b2_std_exposure_value_onbal is None):
            b2_std_exposure_value_onbal = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET'):
            b2_std_crm_tot_inflow_onbal = b2_std_exposure_value_onbal;
        return b2_std_crm_tot_inflow_onbal;

    return b2_std_crm_tot_inflow_onbal;
# end of calculate_b2_std_crm_tot_inflow_onbal()


def calculate_b2_std_crm_tot_inflow_ofbal(data,
                                        b2_std_exposure_value_ofbal):
    b2_std_crm_tot_inflow_ofbal = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_crm_tot_inflow_ofbal;
    else:
        b2_std_ccf = data[2];
        corep_product_category = data[1];

        if (b2_std_ccf is None):
            b2_std_ccf = 0.0;

        if (b2_std_exposure_value_ofbal is None):
            b2_std_exposure_value_ofbal = 0.0;

        b2_std_ccf_exp = 0.0;

        if (corep_product_category == 'OFF BALANCE SHEET'):
            print('\n\n\n\ninside off bal sheet')

            firstelem = 0.0;
            seconelem = 0.0;
            thirdelem = 0.0;

            if (b2_std_ccf == 0.2):
                firstelem = (b2_std_exposure_value_ofbal / b2_std_ccf);

            if (b2_std_ccf == 0.5):
                seconelem = (b2_std_exposure_value_ofbal / b2_std_ccf);

            if (b2_std_ccf == 1):
                thirdelem = (b2_std_exposure_value_ofbal / b2_std_ccf);

            b2_std_crm_tot_inflow_ofbal = (b2_std_ccf_exp + firstelem + seconelem + thirdelem);
        return b2_std_crm_tot_inflow_ofbal;

    return b2_std_crm_tot_inflow_ofbal;
# end of calculate_b2_std_crm_tot_inflow_ofbal()


def calculate_b2_std_rwa_pre_sprt_amt_subexp(data):
    b2_std_rwa_pre_sprt_amt_subexp = 0.0;

    if ((data is None) or (len(data) == 0)):
        return b2_std_rwa_pre_sprt_amt_subexp;
    else:
        crr2_501a_discount_flag = data[3];
        
        if (crr2_501a_discount_flag is None):
            crr2_501a_discount_flag = 'N';

        b2_std_rwa_amt_expone = data[5];
        
        if (b2_std_rwa_amt_expone is None):
            b2_std_rwa_amt_expone = 0.0;

        sme_discount_factor = data[6];
        
        if (sme_discount_factor is None):
            sme_discount_factor = 0.0;

        v_infra_discount_factor = data[7];
        
        if ((v_infra_discount_factor is None) or (v_infra_discount_factor == "")):
            v_infra_discount_factor = 0.75;

        b3_sme_discount_flag = data[8];
        
        if (b3_sme_discount_flag is None):
            b3_sme_discount_flag = 'N';

        b2_std_rwa_pre_sprt_amt_new = 0.0;

        if (crr2_501a_discount_flag == 'Y'):
            if ((v_infra_discount_factor is None) or (v_infra_discount_factor == 0)):
                b2_std_rwa_pre_sprt_amt_new = 1;
            else:
                b2_std_rwa_pre_sprt_amt_new = float(v_infra_discount_factor);

            if ((b2_std_rwa_pre_sprt_amt_new != 0.0) or (b2_std_rwa_pre_sprt_amt_new is not None)):
                b2_std_rwa_pre_sprt_amt_subexp = round(b2_std_rwa_amt_expone, 2) / b2_std_rwa_pre_sprt_amt_new
            else:
                b2_std_rwa_pre_sprt_amt_subexp = 0.0;
        elif (b3_sme_discount_flag == 'Y'):
            if ((sme_discount_factor is None) or (sme_discount_factor == 0)):
                b2_std_rwa_pre_sprt_amt_new = 1;
            else:
                b2_std_rwa_pre_sprt_amt_new = sme_discount_factor;

            if ((b2_std_rwa_pre_sprt_amt_new != 0.0) or (b2_std_rwa_pre_sprt_amt_new is not None)):
                b2_std_rwa_pre_sprt_amt_subexp = round(b2_std_rwa_amt_expone, 2) / b2_std_rwa_pre_sprt_amt_new;
            else:
                b2_std_rwa_pre_sprt_amt_subexp = 0.0;
        else:
            b2_std_rwa_pre_sprt_amt_subexp = round(b2_std_rwa_amt_expone, 2);

        return b2_std_rwa_pre_sprt_amt_subexp;

    return b2_std_rwa_pre_sprt_amt_subexp;
# end of  calculate_b2_std_rwa_pre_sprt_amt_subexp()


def calculate_b2_std_rwa_amt_onbal_subexp(data, b2_std_gtee_cds_rwa):
    b2_std_rwa_amt_onbal_subexp = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_rwa_amt_onbal_subexp;
    else:
        corep_product_category = data[1];
        rwa_post_crm_amount = data[8];

        if (rwa_post_crm_amount is None):
            rwa_post_crm_amount = 0.0;

        if (b2_std_gtee_cds_rwa is None):
            b2_std_gtee_cds_rwa = 0.0;

        if (corep_product_category == 'ON BALANCE SHEET'):
            if (rwa_post_crm_amount <= 0):
                b2_std_rwa_amt_onbal_subexp = rwa_post_crm_amount;
            elif (rwa_post_crm_amount > 0):
                if (b2_std_gtee_cds_rwa >= rwa_post_crm_amount):
                    if (b2_std_gtee_cds_rwa > 0 or b2_std_gtee_cds_rwa < 0):
                        b2_std_rwa_amt_onbal_subexp = (b2_std_gtee_cds_rwa / b2_std_gtee_cds_rwa * rwa_post_crm_amount);
                    elif (b2_std_gtee_cds_rwa < rwa_post_crm_amount):
                        b2_std_rwa_amt_onbal_subexp = b2_std_gtee_cds_rwa;
        return b2_std_rwa_amt_onbal_subexp;

    return b2_std_rwa_amt_onbal_subexp;
# end of calculate_b2_std_rwa_amt_onbal_subexp();


def calculate_b2_std_rwa_pre_sprt_amt_onbal_subexp(data,b2_std_rwa_amt_onbal_subexp):
    b2_std_rwa_pre_sprt_amt_onbal_subexp = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_rwa_pre_sprt_amt_onbal_subexp;
    else:
        crr2_501a_discount_flag = data[4];
        
        if (crr2_501a_discount_flag is None):
            crr2_501a_discount_flag = 'N';

        b2_std_rwa_amt_onbal_subexp = data[5];
        
        if (b2_std_rwa_amt_onbal_subexp is None):
            b2_std_rwa_amt_onbal_subexp = 0.0;

        sme_discount_factor = data[6];
        
        if (sme_discount_factor is None):
            sme_discount_factor = 0.0;

        v_infra_discount_factor = data[7];
        
        if (v_infra_discount_factor is None):
            v_infra_discount_factor = 0.75;

        b3_sme_discount_flag = data[8];
        
        if (b3_sme_discount_flag is None):
            b3_sme_discount_flag = 'N';

        if (crr2_501a_discount_flag == 'Y'):
            b2_std_rwa_pre_sprt_amt_onbal_new = 0.0;

            if (v_infra_discount_factor is None or v_infra_discount_factor == 0):
                b2_std_rwa_pre_sprt_amt_onbal_new = 1
            else:
                b2_std_rwa_pre_sprt_amt_onbal_new = float(v_infra_discount_factor);

            if (b2_std_rwa_pre_sprt_amt_onbal_new is not None or b2_std_rwa_pre_sprt_amt_onbal_new != 0):
                b2_std_rwa_pre_sprt_amt_onbal_subexp = round(b2_std_rwa_amt_onbal_subexp,2) / b2_std_rwa_pre_sprt_amt_onbal_new;
            else:
                b2_std_rwa_pre_sprt_amt_onbal_subexp = 0.0;

        elif (b3_sme_discount_flag == 'Y'):
            b2_std_rwa_pre_sprt_amt_onbal_new = 0.0;

            if (sme_discount_factor is None or sme_discount_factor == 0):
                b2_std_rwa_pre_sprt_amt_onbal_new = 1;
            else:
                b2_std_rwa_pre_sprt_amt_onbal_new = sme_discount_factor;

            if (b2_std_rwa_pre_sprt_amt_onbal_new is not None or b2_std_rwa_pre_sprt_amt_onbal_new != 0):
                b2_std_rwa_pre_sprt_amt_onbal_subexp = round(b2_std_rwa_amt_onbal_subexp,2) / b2_std_rwa_pre_sprt_amt_onbal_new;
            else:
                b2_std_rwa_pre_sprt_amt_onbal_subexp = 0.0;
        else:
            b2_std_rwa_pre_sprt_amt_onbal_subexp = round(b2_std_rwa_amt_onbal_subexp, 2);
        return b2_std_rwa_pre_sprt_amt_onbal_subexp;

    return b2_std_rwa_pre_sprt_amt_onbal_subexp;
# end of calculate_b2_std_rwa_pre_sprt_amt_onbal_subexp() 

def calculate_b2_std_rwa_amt_ofbal_subexp(data, b2_std_gtee_cds_rwa, b2_std_gtee_cds_rwa_agg):
    b2_std_rwa_amt_ofbal = 0.0;
    
    if (data is None or len(data) == 0):
        return b2_std_rwa_amt_ofbal;
    else:
        rwa_post_crm_amount = data[8];
        
        if (rwa_post_crm_amount is None):
            rwa_post_crm_amount = 0.0;

        corep_product_category = data[0];

        if (corep_product_category is None):
            corep_product_category = '';

        if (b2_std_gtee_cds_rwa is None):
            b2_std_gtee_cds_rwa = 0.0;

        if (b2_std_gtee_cds_rwa_agg is None):
            b2_std_gtee_cds_rwa_agg = 0.0;

        if ((corep_product_category is not None) and (corep_product_category == "OFF BALANCE SHEET")):
            if (rwa_post_crm_amount <= 0):
                b2_std_rwa_amt_ofbal = rwa_post_crm_amount;

            if (rwa_post_crm_amount > 0):
                if (b2_std_gtee_cds_rwa >= rwa_post_crm_amount):
                    b2_std_rwa_amt_ofbal = (b2_std_gtee_cds_rwa / b2_std_gtee_cds_rwa_agg * rwa_post_crm_amount);

                if (b2_std_gtee_cds_rwa < rwa_post_crm_amount):
                    b2_std_rwa_amt_ofbal = b2_std_gtee_cds_rwa;
        else:
            b2_std_rwa_amt_ofbal = 0.0;
        return b2_std_rwa_amt_ofbal;

    return b2_std_rwa_amt_ofbal;
# end of calculate_b2_std_rwa_amt_ofbal_subexp()

def calculate_b2_std_rwa_pre_sprt_amt_ofbal_subexp(data,b2_std_rwa_amt_ofbal):
    b2_std_rwa_pre_sprt_amt_ofbal_subexp = 0.0;

    if ((data is None) or (len(data) == 0)):
        return b2_std_rwa_pre_sprt_amt_ofbal_subexp;
    else:
        crr2_501a_discount_flag = data[3];
        
        if (crr2_501a_discount_flag is None):
            crr2_501a_discount_flag = 'N';

        b2_std_rwa_amt_ofbal = data[4];
        
        if (b2_std_rwa_amt_ofbal is None):
            b2_std_rwa_amt_ofbal = 0.0;

        sme_discount_factor = data[5];
        
        if (sme_discount_factor is None):
            sme_discount_factor = 0.0;

        v_infra_discount_factor = data[6];
        
        if (v_infra_discount_factor is None):
            v_infra_discount_factor = 0.75;

        b3_sme_discount_flag = data[7];
        
        if (b3_sme_discount_flag is None):
            b3_sme_discount_flag = 'N';

        if (b2_std_rwa_amt_ofbal is None):
            b2_std_rwa_amt_ofbal = 0.0;

        if (sme_discount_factor is None):
            sme_discount_factor = 0.0;

        if (crr2_501a_discount_flag == 'Y'):
            b2_std_rwa_pre_sprt_amt_ofbal_subexp_new = 0.0;

            if (v_infra_discount_factor is None or v_infra_discount_factor == 0):
                b2_std_rwa_pre_sprt_amt_ofbal_subexp_new = 1;
            else:
                b2_std_rwa_pre_sprt_amt_ofbal_subexp_new = float(v_infra_discount_factor);

            if (b2_std_rwa_pre_sprt_amt_ofbal_subexp_new > 0 or b2_std_rwa_pre_sprt_amt_ofbal_subexp_new < 0):
                b2_std_rwa_pre_sprt_amt_ofbal_subexp = round(b2_std_rwa_amt_ofbal,2) / b2_std_rwa_pre_sprt_amt_ofbal_subexp_new;
            else:
                b2_std_rwa_pre_sprt_amt_ofbal_subexp = 0.0;
        elif (b3_sme_discount_flag == 'Y'):
            b2_std_rwa_pre_sprt_amt_ofbal_subexp_new = 0.0;

            if (sme_discount_factor is None or sme_discount_factor == 0):
                b2_std_rwa_pre_sprt_amt_ofbal_subexp_new = 1;
            else:
                b2_std_rwa_pre_sprt_amt_ofbal_subexp_new = sme_discount_factor;

            if (b2_std_rwa_pre_sprt_amt_ofbal_subexp_new > 0 or b2_std_rwa_pre_sprt_amt_ofbal_subexp_new < 0):
                b2_std_rwa_pre_sprt_amt_ofbal_subexp = round(b2_std_rwa_amt_ofbal,2) / b2_std_rwa_pre_sprt_amt_ofbal_subexp_new;
            else:
                b2_std_rwa_pre_sprt_amt_ofbal_subexp = 0.0;
        else:
            b2_std_rwa_pre_sprt_amt_ofbal_subexp = round(b2_std_rwa_amt_ofbal, 2);

        return b2_std_rwa_pre_sprt_amt_ofbal_subexp

    return b2_std_rwa_pre_sprt_amt_ofbal_subexp
# end of calculate_b2_std_rwa_pre_sprt_amt_ofbal_subexp()


def calculate_std_act_risk_weight_subexp(data, b2_std_gtor_rw_ratio, b2_std_exposure_value):
    std_act_risk_weight_subexp = 0.0;

    if ((data is None) or (len(data) == 0)):
        crr2_501a_discount_flag = 'N'
        sme_discount_factor = 0.0;
        v_infra_discount_factor = 0.75;
        b3_sme_discount_flag = 'N';
        b2_std_rwa_amt_expone = 0.0;
        rwa_approach_code = 'N';
        std_risk_weight_ratio = 0.0;
    else:
        crr2_501a_discount_flag = data[3];
        
        if (crr2_501a_discount_flag is None):
            crr2_501a_discount_flag = 'N';

        sme_discount_factor = data[5];
        
        if (sme_discount_factor is None):
            sme_discount_factor = 0.0;

        v_infra_discount_factor = data[6];
        
        if (v_infra_discount_factor is None):
            v_infra_discount_factor = 0.75;

        b3_sme_discount_flag = data[7];
        
        if (b3_sme_discount_flag is None):
            b3_sme_discount_flag = 'N';

        b2_std_rwa_amt_expone = data[4];
        
        if (b2_std_rwa_amt_expone is None):
            b2_std_rwa_amt_expone = 0.0;        

        rwa_approach_code = data[8];

        if (rwa_approach_code is None):
            rwa_approach_code = 'N';
        
    std_risk_weight_ratio = b2_std_gtor_rw_ratio;

    if (std_risk_weight_ratio is None):
        std_risk_weight_ratio = 0.0;

    if (b2_std_exposure_value is None):
        b2_std_exposure_value = 0.0;

    std_act_risk_weight_subexp_final = 0.0;
    b2_std_rwa_amt_final = 0.0;

    if (b2_std_rwa_amt_expone == 0.00 and b2_std_exposure_value == 0.00):
        std_act_risk_weight_subexp_final = std_risk_weight_ratio
    elif (b2_std_rwa_amt_expone != 0.00 and b2_std_exposure_value == 0.00):
        std_act_risk_weight_subexp_final = 9999
    elif (b2_std_rwa_amt_expone / b2_std_exposure_value > 9999):
        std_act_risk_weight_subexp_final = 9999
    elif (b2_std_rwa_amt_expone < 0 and b2_std_exposure_value >= 0):
        std_act_risk_weight_subexp_final = 9999
    elif (b2_std_rwa_amt_expone > 0 and b2_std_exposure_value < 0):
        std_act_risk_weight_subexp_final = 9999
    else:
        std_act_risk_weight_subexp_new = 0.0;

        if (rwa_approach_code == 'STD' and crr2_501a_discount_flag == 'Y'):
            if (v_infra_discount_factor is None or v_infra_discount_factor == 0):
                std_act_risk_weight_subexp_new = 1;
            else:
                std_act_risk_weight_subexp_new = float(v_infra_discount_factor);

            if (std_act_risk_weight_subexp_new is not None or std_act_risk_weight_subexp_new != 0.0):
                b2_std_rwa_amt_final = b2_std_rwa_amt_expone / std_act_risk_weight_subexp_new;
            else:
                b2_std_rwa_amt_final = 0.0;
        elif (rwa_approach_code == 'STD' and b3_sme_discount_flag == 'Y'):
            if (sme_discount_factor is None or sme_discount_factor == 0):
                std_act_risk_weight_subexp_new = 1;
            else:
                std_act_risk_weight_subexp_new = sme_discount_factor;

            if (std_act_risk_weight_subexp_new is not None or std_act_risk_weight_subexp_new != 0.0):
                b2_std_rwa_amt_final = b2_std_rwa_amt_expone / std_act_risk_weight_subexp_new;
            else:
                b2_std_rwa_amt_final = 0.0;
        else:
            b2_std_rwa_amt_final = b2_std_rwa_amt_expone

        if (b2_std_exposure_value is not None):
            b2_std_rwa_amt_final_final = b2_std_rwa_amt_final / b2_std_exposure_value;
        else:
            b2_std_rwa_amt_final_final = 0;

        if (b2_std_rwa_amt_final_final < 9999 and b2_std_rwa_amt_final_final is not None):
            std_act_risk_weight_subexp_final = b2_std_rwa_amt_final_final
        else:
            std_act_risk_weight_subexp_final = 9999;

    if (float(std_act_risk_weight_subexp_final) is not None):
        std_act_risk_weight_subexp = round(float(std_act_risk_weight_subexp_final), 2);
    else:
        std_act_risk_weight_subexp = 0.0;

    return std_act_risk_weight_subexp;
# end of calculate_std_act_risk_weight_subexp()


def calculate_b2_std_fully_adj_exp_ccf_100_subexp(data,b2_std_exposure_value_ofbal):
    b2_std_fully_adj_exp_ccf_100_subexp = 0.0;

    if (data is None or len(data) == 0):
        return b2_std_fully_adj_exp_ccf_100_subexp;
    else:
        b2_std_ccf = data[2];

        if (b2_std_ccf is None):
            b2_std_ccf = 0.0;

        if (b2_std_exposure_value_ofbal is None):
            b2_std_exposure_value_ofbal = 0.0;

        if (b2_std_ccf == 1):
            b2_std_fully_adj_exp_ccf_100_subexp = (b2_std_exposure_value_ofbal / b2_std_ccf);

        return b2_std_fully_adj_exp_ccf_100_subexp;

    return b2_std_fully_adj_exp_ccf_100_subexp;
# end of calculate_b2_std_fully_adj_exp_ccf_100_subexp()


def calculate_external_counterparty_id(external_counterparty_id_temp, csys_id, csys_code):
    external_counterparty_id = ""
    
    if external_counterparty_id_temp is None:
        if csys_id is None:
            csys_id = "";
        if csys_code is None:
            csys_code = "";
        external_counterparty_id = csys_id + '_' + csys_code;
    else:
        external_counterparty_id = external_counterparty_id_temp;

    return external_counterparty_id
#end of calculate_b2_irb_crm_lip_amt()

def calculate_crd4_memo_basel_exposure_type(memo_b2_std_basel_exposure_type,crd4_memo_items_exposure_type,
                                            type_value):
    CRD4_MEMO_BASEL_EXPOSURE_TYPE = '';

    if (crd4_memo_items_exposure_type is None):
        crd4_memo_items_exposure_type = '';

    if (memo_b2_std_basel_exposure_type == 'Equities'):

        crd4_memo_basel_exposure_type = 'Equity Claims';
    elif (memo_b2_std_basel_exposure_type == 'Central Governments and Central Banks'):
        crd4_memo_basel_exposure_type = 'Exposures to central governments or central banks';
    elif (memo_b2_std_basel_exposure_type == 'Corporates'):
        crd4_memo_basel_exposure_type = 'Exposures to corporates';
    elif (memo_b2_std_basel_exposure_type == 'Institutions'):
        crd4_memo_basel_exposure_type = 'Exposures to institutions';
    elif (memo_b2_std_basel_exposure_type == 'Non-credit Obligation Assets'):
        crd4_memo_basel_exposure_type = 'Other items';       
    elif (memo_b2_std_basel_exposure_type == 'Retail'):
        crd4_memo_basel_exposure_type = 'Retail exposures';
    else:
        crd4_memo_basel_exposure_type = memo_b2_std_basel_exposure_type;

    return crd4_memo_basel_exposure_type;

#end of calculate_crd4_memo_basel_exposure_type

def calculate_b2_std_crm_tot_outflow_rwa(b2_std_crm_tot_outflow_rwa_agg, rwa_post_crm_amount):
    b2_std_crm_tot_outflow_rwa = 0.0
    
    if(rwa_post_crm_amount is None):
        rwa_post_crm_amount = 0.0
        
    if(b2_std_crm_tot_outflow_rwa_agg is None):
        b2_std_crm_tot_outflow_rwa_agg = 0.0

    if(b2_std_crm_tot_outflow_rwa_agg > 0.0):
        if(rwa_post_crm_amount <= 0.0):
            b2_std_crm_tot_outflow_rwa = rwa_post_crm_amount
        else:
            b2_std_crm_tot_outflow_rwa = min(b2_std_crm_tot_outflow_rwa_agg, rwa_post_crm_amount )
    return b2_std_crm_tot_outflow_rwa
#end of calculate_b2_std_crm_tot_outflow_rwa

def enrich_exposure_row(further_enriched_final_exposures, column):
    enrich_df = further_enriched_final_exposures
    cols = set(["B2_IRB_GTEE_AMT", "SEC_DISCNTD_B2_IRB_GARNTEE_AMT", "SEC_DISCNTD_B2_IRB_COLLTRL_AMT", "B2_IRB_COLL_AMT",
            "B2_IRB_CRM_UFCP_CRED_DERIV_AMT", "B2_IRB_CRM_FCP_OTH_FUNDED_AMT", "B2_IRB_CRM_COD_AMT","B2_IRB_CRM_UFCP_GUAR_AMT",
            "B2_IRB_CRM_LIP_AMT","B2_IRB_CRM_FCP_RECEIVABLES_AMT","B2_IRB_CRM_FCP_OTH_PHY_COL_AMT" , "CRM_UFCP_GUAR_AMT_AGG", 
            "B2_IRB_CRM_FCP_ELG_FIN_COL_AMT", "B2_IRB_CRM_FCP_REAL_ESTATE_AMT", "B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT"])
            
    cols = set(column.lower() for column in cols)

    if (column in cols):
        enrich_df = enrich_df.withColumn(column, lit(0).cast(DoubleType()));
    else:
        enrich_df = enrich_df.withColumn(column, lit(None));

    return  enrich_df
#end of enrich_exposure_row    

def calculate_crm_fcp_real_estate_amt_indv(corep_crm_category, double_default_flag, b2_irb_coll_amt):
    crm_fcp_real_estate_amt = 0.0;    
    
    if (b2_irb_coll_amt is None):
        b2_irb_coll_amt = 0.0;
        
    if (corep_crm_category is None):
        corep_crm_category = "";
        
    if (double_default_flag is None):
        double_default_flag = 'N';
        
    intrmdt_amt = 0.0;
        
    if (double_default_flag.upper() == 'Y'):
        intrmdt_amt = 0.0;
    else:

        intrmdt_amt = b2_irb_coll_amt;
                     
    if (corep_crm_category.upper() == 'REAL ESTATE'):
        crm_fcp_real_estate_amt = intrmdt_amt;
    else:
        crm_fcp_real_estate_amt = 0.0;
        
    return crm_fcp_real_estate_amt;
        
# end of calculate_crm_fcp_real_estate_amt_indv()

def enrich_corep_exposure_staging(prev_qarter_value_boradcast_obj):
    broadcasted_prev_qrtr = list(prev_qarter_value_boradcast_obj.toLocalIterator());

    prev_qrtr_dict = {};

    for m in broadcasted_prev_qrtr:
        prev_qrtr_dict[m[0]] = m[1];

    def iterateOverRows(rows):
        returned_list = [];
        for x in rows:
            current_row = data_cleansing(x)
            reporting_reg_code = current_row[0];
            model_code = current_row[1];
            b2_irb_ac_basel_exposure_type = current_row[2];
            b2_irb_ac_reg_exposure_sub_type = current_row[3];
            division_sub2 = current_row[4];
            deal_id = current_row[5];
            approved_app_code = current_row[6];
            year_month = current_row[7];
            b2_std_ac_basel_expo_typ = current_row[8];
            reporting_type_code = current_row[9];
            b2_std_rsk_wght_ratio = current_row[10];
            basel_data_flag = current_row[11];
            citizens_override_flag = current_row[12];
            b2_std_ead_post_crm_amt = current_row[13];
            b2_app_ead_post_crm_amt = current_row[14];
            external_counterparty_id_temp = current_row[15];
            ifrs9_tot_regu_eclamt_gbp = current_row[16];
            provision_amt_gbp = current_row[17];
            corep_product_category = current_row[18];
            b3_cva_provision_amt = current_row[19];
            b2_std_rwa_post_crm_amt = current_row[20];
            b2_app_rwa_post_crm_amt = current_row[21];
            retail_off_bal_sheet_flag = current_row[22];
            b2_std_drawn_rwa = current_row[23];
            b2_std_undrawn_rwa = current_row[24];
            aclm_product_type = current_row[25];
            b2_irb_undrawn_rwa = current_row[26];
            b2_app_adjusted_pd_ratio = current_row[27];
            b2_app_adjusted_lgd_ratio = current_row[28];
            b2_irb_asset_class_reg_exposure_sub_type = current_row[29];
            b2_irb_asset_class_basel_exposure_type = current_row[30];
            DEFINITIVE_SLOT_CATEGORY_CALC = current_row[31];
            regulatory_product_type = current_row[32];
            PD_MODEL_CODE = current_row[33];
            b2_irb_asset_class_reg_exposure_type = current_row[34];
            CRR2_501A_DISCOUNT_FLAG = current_row[35];
            v_infra_discount_factor = current_row[36];
            B3_SME_DISCOUNT_FLAG = current_row[37];
            SME_DISCOUNT_FACTOR = current_row[38];
            approved_approach_code = current_row[39];
            REPORTING_REGULATOR_CODE = current_row[40];
            COREP_IRB_EXPOSURE_SUB_TYPE = current_row[41];
            b2_irb_asset_class_basel_exposure_sub_type = current_row[42];
            TYPE = current_row[43];
            dac_type = current_row[44];
            dac_valid_flag = current_row[45];
            dac_asset_class_rk = current_row[46];
            b2_irb_drawn_ead = current_row[47];
            b2_irb_undrawn_ead = current_row[48];
            bank_base_role_code = current_row[49];
            off_bal_led_exp_gbp_post_sec = current_row[50];
            undrawn_commitment_amt_pst_sec = current_row[51];
            off_balance_exposure = current_row[52];
            undrawn_commitment_amt = current_row[53];
            b2_irb_effective_maturity_yrs = current_row[54];
            b2_std_ead_pre_crm_amt = current_row[55];
            b2_app_ead_pre_crm_amt = current_row[56];
            b2_irb_drawn_expected_loss = current_row[57];
            b2_app_expected_loss_amt = current_row[58];
            fi_avc_category = current_row[59];
            b2_irb_undrawn_expected_loss = current_row[60];
            obligors_count = current_row[61];
            dim_pd_band_code = current_row[62];
            dim_mgs = current_row[63];
            residual_maturity_days = current_row[64];
            ec92_code = current_row[65];
            bsd_marker = current_row[66];
            sector_cluster = current_row[67];
            sub_sector = current_row[68];
            b2_std_pre_dflt_asset_class_basel_exposure_type = current_row[69];
            memo_b2_std_basel_exposure_type = current_row[70];
            external_counterparty_qccp_flag = current_row[71];
            b2_std_ccf_input = current_row[72];
            grdw_pool_id = current_row[73];
            comm_less_than_1_year = current_row[74];
            comm_more_than_1_year = current_row[75];
            b2_std_orig_exp_pre_con_factor = current_row[76];
            csys_id = current_row[77];
            csys_code = current_row[78];
            memo_b2_std_basel_exposure_sub_type = current_row[79];
            memo_b2_std_type = current_row[80];
            memo_b2_std_reg_exposure_type = current_row[81];
            type_value = current_row[82];
            unsettled_price_difference_amt = current_row[83];
            internal_transaction_flag = current_row[84];
            override_value = current_row[85];
            v_icb_eff_from_year_month_rk = current_row[86];
            risk_on_gs_code= current_row[87];
            risk_taker_group_structure= current_row[88];
            risk_on_group_structure= current_row[89];
            risk_taker_gs_code= current_row[90];
            dim_ctrpty_risk_entity_class= current_row[91];
            risk_taker_ll_rru = current_row[92];
            v_core_ig_eff_year_month_rk = current_row[93];
            rwa_calc_method = current_row[94];
            aiml_product_type = current_row[95];
            cost_centre = current_row[96];
            bspl_900_gl_code = current_row[97];
            net_exposure = current_row[98];
            bspl_60_gl_code = current_row[99];
            drawn_ledger_balance = current_row[100];
            b2_irb_interest_at_default_gbp = current_row[101];
            accrued_interest_gbp = current_row[102];
            e_star_gross_exposure = current_row[103];
            crd4_rep_irbeq_reg_exposure_type = current_row[104];
            obligor_type = current_row[105];

            rwa_approach_code = calculate_rwa_approach_code(reporting_reg_code, model_code,
                                                            b2_irb_ac_basel_exposure_type,
                                                            b2_irb_ac_reg_exposure_sub_type, division_sub2,
                                                            approved_app_code);

            ig_exclusion_flag = calculate_ig_exclusion_flag(reporting_reg_code, internal_transaction_flag, override_value, v_icb_eff_from_year_month_rk, year_month, risk_on_gs_code, risk_taker_group_structure, risk_on_group_structure, risk_taker_gs_code, dim_ctrpty_risk_entity_class, risk_taker_ll_rru, v_core_ig_eff_year_month_rk);

            corep_std_ac_basl_expo_typ = calculate_corep_ac_bsl_expo_typ(b2_std_ac_basel_expo_typ, reporting_type_code);

            std_rsk_wght_ratio = calculate_std_rsk_wght_ratio(rwa_approach_code, b2_std_rsk_wght_ratio);

            crd4_rep_std_flag = calculate_crd4_rep_std_flag(rwa_approach_code, corep_std_ac_basl_expo_typ,
                                                            basel_data_flag);

            calculate_rep_provision_amt_gbp(external_counterparty_id_temp, deal_id, ifrs9_tot_regu_eclamt_gbp,
                                            provision_amt_gbp);            

            rep_provision_amt_gbp = calculate_rep_provision_amt_gbp(external_counterparty_id_temp, deal_id,
                                                                    ifrs9_tot_regu_eclamt_gbp,
                                                                    provision_amt_gbp);
            if (rep_provision_amt_gbp is not None):
                b2_irb_provisions_amt = rep_provision_amt_gbp;
            else:
                b2_irb_provisions_amt = 0.0;

            b2_std_provisions_amt_onbal = calculate_b2_std_provisions_amt_onbal(corep_product_category,
                                                                                b3_cva_provision_amt,
                                                                                rep_provision_amt_gbp);

            rwa_post_crm_amt = calculate_rwa_post_crm_amt(citizens_override_flag, b2_std_rwa_post_crm_amt,
                                                        b2_app_rwa_post_crm_amt);

            ret_rwa_post_crm_amt_pct = float(calculate_ret_rwa_post_crm_amt_pct(b2_std_drawn_rwa, b2_std_undrawn_rwa,
                                                                                retail_off_bal_sheet_flag));

            b2_std_rwa_post_crm_amt_onbal = calculate_b2_std_rwa_post_crm_amt_onbal(corep_product_category,
                                                                                    retail_off_bal_sheet_flag,
                                                                                    citizens_override_flag,
                                                                                    b2_std_rwa_post_crm_amt,
                                                                                    b2_app_rwa_post_crm_amt,
                                                                                    ret_rwa_post_crm_amt_pct,
                                                                                    aclm_product_type);

            b2_irb_rwa_amt_ofbal = calculate_b2_irb_rwa_amt_ofbal(aclm_product_type, retail_off_bal_sheet_flag,
                                                                b2_app_rwa_post_crm_amt, b2_irb_undrawn_rwa);

            ead_pd_numerator = calculate_ead_pd_numerator(b2_app_adjusted_pd_ratio, b2_app_ead_post_crm_amt);

            ead_lgd_numerator = calculate_ead_lgd_numerator(b2_app_adjusted_lgd_ratio, b2_app_ead_post_crm_amt);

            crd4_rep_irbeq_flag = calculate_crd4_rep_irbeq_flag(crd4_rep_irbeq_reg_exposure_type,
                                                                rwa_approach_code, 
                                                                basel_data_flag);

            b2_irb_ead_amt_onbal = calculate_b2_irb_ead_amt_onbal(aclm_product_type, retail_off_bal_sheet_flag,
                                                                b2_irb_drawn_ead, b2_app_ead_post_crm_amt);

            ead_pd_numerator_on = b2_app_adjusted_pd_ratio * b2_irb_ead_amt_onbal;

            b2_irb_ead_amt_ofbal = calculate_b2_irb_ead_amt_ofbal(aclm_product_type, retail_off_bal_sheet_flag,
                                                                b2_irb_undrawn_ead, b2_app_ead_post_crm_amt);

            ead_pd_numerator_off = b2_app_adjusted_pd_ratio * b2_irb_ead_amt_ofbal;

            irb_orig_exp_pre_conv_ofbal = calculate_irb_orig_exp_pre_conv_ofbal(aclm_product_type, bank_base_role_code,
                                                                                off_bal_led_exp_gbp_post_sec,
                                                                                undrawn_commitment_amt_pst_sec,
                                                                                off_balance_exposure,
                                                                                undrawn_commitment_amt);

            ead_lgd_numerator_on = b2_app_adjusted_lgd_ratio * b2_irb_ead_amt_onbal;

            ead_lgd_numerator_off = b2_app_adjusted_lgd_ratio * b2_irb_ead_amt_ofbal;

            ead_mat_years_numerator = b2_irb_effective_maturity_yrs * b2_app_ead_post_crm_amt;

            ead_mat_years_numerator_on = b2_irb_effective_maturity_yrs * b2_irb_ead_amt_onbal;

            ead_mat_years_numerator_off = b2_irb_effective_maturity_yrs * b2_irb_ead_amt_ofbal;

            ead_pre_crm_amt = calculate_ead_pre_crm_amt(citizens_override_flag, b2_std_ead_pre_crm_amt,
                                                        b2_app_ead_pre_crm_amt)

            b2_irb_el_amt_onbal = calculate_b2_irb_el_amt_onbal(aclm_product_type, retail_off_bal_sheet_flag,
                                                                b2_irb_drawn_expected_loss, b2_app_expected_loss_amt)

            # obligor_type = calculate_obligor_type(b2_app_ead_post_crm_amt, external_counterparty_id_temp)

            lrfe_ufe_flag = calculate_lrfe_ufe_flag(fi_avc_category)

            b2_irb_el_amt_ofbal = calculate_b2_irb_el_amt_ofbal(aclm_product_type, retail_off_bal_sheet_flag,
                                                                b2_irb_undrawn_expected_loss, b2_app_expected_loss_amt)

            retail_obligor_count = calculate_retail_obligor_count(obligor_type, obligors_count)

            pd_band_code = calculate_pd_band_code(division_sub2, approved_approach_code, dim_pd_band_code)

            mgs = calculate_mgs(division_sub2, approved_approach_code, dim_mgs)

            sme_flag = calculate_sme_flag(b2_irb_asset_class_reg_exposure_sub_type)

            slotting_residual_maturity = calculate_slotting_residual_maturity(residual_maturity_days)

            risk_type = calculate_risk_type(reporting_type_code, aclm_product_type)

            c33_exposure_sector = calculate_c33_exposure_sector(ec92_code, bsd_marker, external_counterparty_id_temp,
                                                                sector_cluster, sub_sector)

            crd4_rep_irb_flag = calculate_crd4_rep_irb_flag(b2_irb_asset_class_reg_exposure_type, rwa_calc_method,
                                                            COREP_IRB_EXPOSURE_SUB_TYPE,
                                                            b2_irb_asset_class_basel_exposure_sub_type, basel_data_flag,
                                                            rwa_approach_code, dac_type, dac_valid_flag)

            crd4_memo_items_exposure_type = calculate_crd4_memo_items_exposure_type(b2_std_ac_basel_expo_typ,
                                                                                    b2_std_pre_dflt_asset_class_basel_exposure_type,
                                                                                    b2_irb_asset_class_reg_exposure_type)

            crd4_memo_items_asset_class_basel_exposure_type = calculate_crd4_memo_items_asset_class_basel_exposure_type(memo_b2_std_basel_exposure_type,
                                            crd4_memo_items_exposure_type,
                                            memo_b2_std_type)

            if (reporting_reg_code is None):
                reporting_reg_code == "";

            data = prev_qrtr_dict.get(deal_id);

            returned_attribute_array = derived_prev_quarter_atributes(data)

            b2_irb_obsrvd_new_default_flag = calculate_b2_irb_obsrvd_new_default_flag(external_counterparty_id_temp,
                                                                                    approved_approach_code,
                                                                                    b2_app_adjusted_pd_ratio,
                                                                                    returned_attribute_array[1]);

            b2_std_obsrvd_new_default_flag = calculate_b2_std_obsrvd_new_default_flag(external_counterparty_id_temp,
                                                                                    approved_approach_code,
                                                                                    b2_irb_asset_class_basel_exposure_type,
                                                                                    returned_attribute_array[0]);

            default_fund_contrib_indicator = calculate_default_fund_contrib_indicator(regulatory_product_type);

            qccp_flag = calculate_qccp_flag(external_counterparty_qccp_flag);

            b2_std_provisions_amt = calculate_b2_std_provisions_amt(rep_provision_amt_gbp, b3_cva_provision_amt);

            b2_std_provisions_amt_ofbal = calculate_b2_std_provisions_amt_ofbal(corep_product_category,
                                                                                rep_provision_amt_gbp,
                                                                                b3_cva_provision_amt);

            b2_std_ead_post_crm_amt_ofbal = calculate_b2_std_ead_post_crm_amt_ofbal(corep_product_category,
                                                                                    retail_off_bal_sheet_flag,
                                                                                    citizens_override_flag,
                                                                                    b2_std_ead_post_crm_amt,
                                                                                    b2_app_ead_post_crm_amt,
                                                                                    ret_rwa_post_crm_amt_pct);

            b2_std_ccf_exp = calculate_b2_std_ccf_exp(rwa_approach_code, corep_product_category,
                                                    retail_off_bal_sheet_flag,
                                                    ret_rwa_post_crm_amt_pct,
                                                    citizens_override_flag,
                                                    b2_std_ead_post_crm_amt,
                                                    b2_app_ead_post_crm_amt,
                                                    b2_std_ccf_input,
                                                    undrawn_commitment_amt,
                                                    off_balance_exposure);

            std_orig_exp_pre_conv_ofbal = calculate_std_orig_exp_pre_conv_ofbal(aclm_product_type,
                                                                                bank_base_role_code,
                                                                                grdw_pool_id,
                                                                                off_bal_led_exp_gbp_post_sec,
                                                                                comm_less_than_1_year,
                                                                                comm_more_than_1_year,
                                                                                undrawn_commitment_amt_pst_sec,
                                                                                off_balance_exposure,
                                                                                undrawn_commitment_amt);

            b2_std_fully_adj_exp_ccf_0 = calculate_b2_std_fully_adj_exp_ccf_0(b2_std_ccf_exp,
                                                                            std_orig_exp_pre_conv_ofbal);

            b2_std_ead_post_crm_amt_onbal = calculate_b2_std_ead_post_crm_amt_onbal(corep_product_category,
                                                                                    retail_off_bal_sheet_flag,
                                                                                    citizens_override_flag,
                                                                                    b2_std_ead_post_crm_amt,
                                                                                    b2_app_ead_post_crm_amt,
                                                                                    ret_rwa_post_crm_amt_pct,
                                                                                    aclm_product_type);

            b2_std_orig_exp_pre_con_factor_exp = calculate_b2_std_orig_exp_pre_con_factor_exp(aclm_product_type,
                                                                                            b2_std_orig_exp_pre_con_factor,
                                                                                            b3_cva_provision_amt);

            std_orig_exp_pre_conv_onbal = calculate_std_orig_exp_pre_conv_onbal(corep_product_category,
                                                                                b2_std_orig_exp_pre_con_factor_exp,
                                                                                std_orig_exp_pre_conv_ofbal);

            rwa_post_crm_amount = calculate_rwa_post_crm_amount(citizens_override_flag, b2_app_rwa_post_crm_amt,
                                                                b2_std_rwa_post_crm_amt);

            external_counterparty_id = calculate_external_counterparty_id(external_counterparty_id_temp, csys_id,
                                                                        csys_code);
            
            crd4_memo_items_asset_class_reg_exposure_type = calculate_crd4_memo_items_asset_class_reg_exposure_type(memo_b2_std_reg_exposure_type, memo_b2_std_basel_exposure_type,
                                            crd4_memo_items_exposure_type,
                                            memo_b2_std_type)
            
            crd4_memo_items_asset_class_basel_exposure_sub_type = calculate_crd4_memo_items_asset_class_basel_exposure_sub_type(memo_b2_std_basel_exposure_type,
                                            crd4_memo_items_exposure_type,
                                            memo_b2_std_type, memo_b2_std_basel_exposure_sub_type);
            crd4_memo_basel_exposure_type = calculate_crd4_memo_basel_exposure_type(memo_b2_std_basel_exposure_type,
                                                                                    crd4_memo_items_exposure_type,
                                                                                    type_value);
        
            ra_ad_exp = calculate_ra_ad_exp(aclm_product_type, b2_app_ead_post_crm_amt, aiml_product_type, cost_centre, bspl_900_gl_code, net_exposure, bspl_60_gl_code, off_balance_exposure, drawn_ledger_balance, b2_irb_interest_at_default_gbp,accrued_interest_gbp, approved_approach_code, rep_provision_amt_gbp , e_star_gross_exposure, basel_data_flag , internal_transaction_flag)
        
            new_row = Row(REPORTING_REGULATOR_CODE=reporting_reg_code,
                        RWA_APPROACH_CODE=rwa_approach_code,
                        DEAL_ID=deal_id, IG_EXCLUSION_FLAG=ig_exclusion_flag,
                        YEAR_MONTH=year_month, COREP_STD_ASSET_CLASS_BASEL_EXPOSURE_TYPE=corep_std_ac_basl_expo_typ,
                        STD_RISK_WEIGHT_RATIO=std_rsk_wght_ratio,
                        CRD4_REP_STD_FLAG=crd4_rep_std_flag,
                        REP_PROVISION_AMT_GBP=rep_provision_amt_gbp, B2_IRB_PROVISIONS_AMT=b2_irb_provisions_amt,
                        B2_STD_PROVISIONS_AMT_ONBAL=b2_std_provisions_amt_onbal,
                        RWA_POST_CRM_AMOUNT=rwa_post_crm_amt,
                        B2_STD_RWA_POST_CRM_AMT_ONBAL=b2_std_rwa_post_crm_amt_onbal,
                        RET_RWA_POST_CRM_AMT_PCT=ret_rwa_post_crm_amt_pct,
                        B2_IRB_RWA_AMT_OFBAL=b2_irb_rwa_amt_ofbal,
                        EAD_PD_NUMERATOR=ead_pd_numerator,
                        EAD_LGD_NUMERATOR=ead_lgd_numerator,
                        CRD4_REP_IRBEQ_FLAG=crd4_rep_irbeq_flag, B2_IRB_EAD_AMT_ONBAL=b2_irb_ead_amt_onbal,
                        EAD_PD_NUMERATOR_ON=ead_pd_numerator_on, B2_IRB_EAD_AMT_OFBAL=b2_irb_ead_amt_ofbal,
                        EAD_PD_NUMERATOR_OFF=ead_pd_numerator_off,
                        IRB_ORIG_EXP_PRE_CONV_OFBAL=irb_orig_exp_pre_conv_ofbal,
                        EAD_LGD_NUMERATOR_ON=ead_lgd_numerator_on, EAD_LGD_NUMERATOR_OFF=ead_lgd_numerator_off,
                        EAD_MAT_YEARS_NUMERATOR=ead_mat_years_numerator,
                        EAD_MAT_YEARS_NUMERATOR_ON=ead_mat_years_numerator_on,
                        EAD_MAT_YEARS_NUMERATOR_OFF=ead_mat_years_numerator_off,
                        EAD_PRE_CRM_AMT=ead_pre_crm_amt,
                        B2_IRB_EL_AMT_ONBAL=b2_irb_el_amt_onbal,
                        #   OBLIGOR_TYPE=obligor_type,
                        LRFE_UFE_FLAG=lrfe_ufe_flag,
                        B2_IRB_EL_AMT_OFBAL=b2_irb_el_amt_ofbal,
                        RETAIL_OBLIGOR_COUNT=retail_obligor_count,
                        PD_BAND_CODE=pd_band_code,
                        MGS=mgs,
                        SME_FLAG=sme_flag,
                        SLOTTING_RESIDUAL_MATURITY=slotting_residual_maturity,
                        RISK_TYPE=risk_type,
                        C33_EXPOSURE_SECTOR=c33_exposure_sector,
                        CRD4_REP_IRB_FLAG=crd4_rep_irb_flag,
                        CRD4_MEMO_ITEMS_EXPOSURE_TYPE=crd4_memo_items_exposure_type,
                        CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_TYPE=crd4_memo_items_asset_class_basel_exposure_type,
                        STD_PRE_DFLT=returned_attribute_array[0],
                        AIRB_PRE_DFLT=returned_attribute_array[1],
                        B2_IRB_OBSRVD_NEW_DEFAULT_FLAG=b2_irb_obsrvd_new_default_flag,
                        B2_STD_OBSRVD_NEW_DEFAULT_FLAG=b2_std_obsrvd_new_default_flag,
                        DEFAULT_FUND_CONTRIB_INDICATOR=default_fund_contrib_indicator,
                        QCCP_FLAG=qccp_flag,
                        B2_STD_PROVISIONS_AMT=b2_std_provisions_amt,
                        B2_STD_PROVISIONS_AMT_OFBAL=b2_std_provisions_amt_ofbal,
                        B2_STD_EAD_POST_CRM_AMT_OFBAL=b2_std_ead_post_crm_amt_ofbal,
                        B2_STD_CCF=b2_std_ccf_exp,
                        B2_STD_FULLY_ADJ_EXP_CCF_0=b2_std_fully_adj_exp_ccf_0,
                        B2_STD_EAD_POST_CRM_AMT_ONBAL=b2_std_ead_post_crm_amt_onbal,
                        B2_STD_ORIG_EXP_PRE_CON_FACTOR_EXP=b2_std_orig_exp_pre_con_factor_exp,
                        STD_ORIG_EXP_PRE_CONV_ONBAL=std_orig_exp_pre_conv_onbal,
                        EXTERNAL_COUNTERPARTY_ID=external_counterparty_id,
                        CRD4_MEMO_ITEMS_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE = crd4_memo_items_asset_class_basel_exposure_sub_type,
                        CRD4_MEMO_ITEMS_ASSET_CLASS_REG_EXPOSURE_TYPE = crd4_memo_items_asset_class_reg_exposure_type,
                        CRD4_MEMO_BASEL_EXPOSURE_TYPE = crd4_memo_basel_exposure_type,
                        UNSETTLED_PRICE_DIFFERENCE_AMT = unsettled_price_difference_amt,
                        RA_AD_EXP = ra_ad_exp,
                        STD_ORIG_EXP_PRE_CONV_OFBAL = std_orig_exp_pre_conv_ofbal
                        );
            returned_list.append(new_row);
        return returned_list;

    return iterateOverRows;
#	end of enrich_corep_exposure_staging()

def enrich_corep_sub_exposure_staging(rows):
    returned_list = [];
    for x in rows:
        current_row = data_cleansing_for_sub_exposures(x)
        reporting_reg_code = current_row[0];
        deal_id = current_row[1];
        year_month = current_row[2];
        b2_std_eff_gtee_amt = current_row[3];
        b2_std_eff_cds_amt = current_row[4];
        type_value = current_row[5];
        b2_std_gtee_cds_rwa = current_row[6];
        mitigant_type = current_row[7];
        corep_crm_category = current_row[8];
        sec_discntd_b2_irb_garntee_amt = current_row[9];
        double_default_flag = current_row[10];
        b2_irb_gtee_amt = current_row[11];
        sec_discntd_b2_irb_colltrl_amt = current_row[12];
        b2_irb_coll_amt = float(current_row[13]);
        b2_std_eff_coll_amt_post_hcut = current_row[14];
        b2_std_fx_haircut = current_row[15];
        b2_std_elig_coll_amt = current_row[16];
        collateral_type_code = current_row[17];
        sys_protection_type = current_row[18];
        sys_code = current_row[19];
        mitigant_id = current_row[20];
        b2_std_coll_volatility_amt = current_row[21];
        b2_std_coll_mty_mismatch_amt = current_row[22];
        b2_irb_colltrl_amt_ttc = current_row[23];
        b2_irb_garntee_amt_ttc = current_row[24];
        sec_discnt_b2_irb_coll_amt_ttc = current_row[25];
        sec_discnt_b2_irb_gtee_amt_ttc = current_row[26];
        rwa_post_crm_amount = current_row[27];
        ead_post_crm_amt = current_row[28];

    
        b2_std_crm_tot_outflow_rwa_indv = calculate_b2_std_crm_tot_outflow_rwa_indv(b2_std_eff_gtee_amt,
                                                                                    b2_std_eff_cds_amt, type_value,
                                                                                    mitigant_type, b2_std_gtee_cds_rwa);

        b2_std_crm_tot_outflow_indv = calculate_b2_std_crm_tot_outflow_indv(b2_std_eff_gtee_amt,
                                                                            b2_std_eff_cds_amt, type_value,
                                                                            mitigant_type);

        b2_irb_crm_ufcp_guar_amt = calculate_b2_irb_crm_ufcp_guar_amt(corep_crm_category,
                                                                    sec_discntd_b2_irb_garntee_amt,
                                                                    double_default_flag, b2_irb_gtee_amt);

        b2_irb_crm_ufcp_cred_deriv_amt = calculate_b2_irb_crm_ufcp_cred_deriv_amt(corep_crm_category, mitigant_type,
                                                                                sec_discntd_b2_irb_garntee_amt,
                                                                                double_default_flag, b2_irb_gtee_amt);

        crm_ufcp_cred_deriv_amt_indv = calculate_crm_ufcp_cred_deriv_amt(corep_crm_category, double_default_flag,
                                                                        mitigant_type, b2_irb_gtee_amt);

        crm_ufcp_guar_amt_indv = calculate_crm_ufcp_guar_amt(corep_crm_category, double_default_flag,
                                                            b2_irb_gtee_amt);

        crm_fcp_elig_fin_coll_amt_indv = calculate_crm_fcp_elig_fin_coll_amt(corep_crm_category, double_default_flag,
                                                                            b2_irb_coll_amt);

        b2_irb_crm_fcp_oth_funded_amt = calculate_b2_irb_crm_fcp_oth_funded_amt(corep_crm_category,
                                                                                sec_discntd_b2_irb_colltrl_amt,
                                                                                double_default_flag, b2_irb_coll_amt);

        b2_irb_crm_fcp_real_estate_amt = calculate_b2_irb_crm_fcp_real_estate_amt(corep_crm_category,
                                                                                double_default_flag,
                                                                                b2_irb_coll_amt);

        b2_irb_crm_fcp_oth_phy_col_amt = calculate_b2_irb_crm_fcp_oth_phy_col_amt(corep_crm_category,
                                                                                double_default_flag,
                                                                                b2_irb_coll_amt);

        b2_irb_crm_fcp_receivables_amt = calculate_b2_irb_crm_fcp_receivables_amt(corep_crm_category,
                                                                                double_default_flag,
                                                                                b2_irb_coll_amt);

        b2_std_eff_fin_coll_amt = calculate_b2_std_eff_fin_coll_amt(corep_crm_category, b2_std_eff_coll_amt_post_hcut);

        b2_std_oth_fun_cred_prot_amt = calculate_b2_std_oth_fun_cred_prot_amt(corep_crm_category,
                                                                            b2_std_eff_coll_amt_post_hcut);

        b2_std_coll_fx_haircut_amt = calculate_b2_std_coll_fx_haircut_amt(b2_std_fx_haircut, b2_std_elig_coll_amt);

        blank_crm_amt_std = calculate_blank_crm_amt_std(corep_crm_category, b2_std_eff_gtee_amt, b2_std_eff_cds_amt,
                                                        b2_std_eff_coll_amt_post_hcut);

        blank_crm_amt_irb = calculate_blank_crm_amt_irb(corep_crm_category, b2_irb_gtee_amt, b2_irb_coll_amt);

        b2_irb_crm_cod_amt = calculate_b2_irb_crm_cod_amt(collateral_type_code, sys_protection_type, sys_code,
                                                        corep_crm_category, sec_discntd_b2_irb_colltrl_amt,
                                                        double_default_flag, b2_irb_coll_amt);

        b2_irb_crm_lip_amt = calculate_b2_irb_crm_lip_amt(collateral_type_code, sys_protection_type, sys_code,
                                                        corep_crm_category, sec_discntd_b2_irb_colltrl_amt,
                                                        double_default_flag, b2_irb_coll_amt);

        if (b2_std_gtee_cds_rwa is not None):
            b2_std_gtee_cds_rwa_indv = b2_std_gtee_cds_rwa;
        else:
            b2_std_gtee_cds_rwa_indv = 0.0;

        if(b2_std_eff_gtee_amt is None):
            b2_std_eff_gtee_amt = 0.0;

        if(b2_std_eff_cds_amt is None):
            b2_std_eff_cds_amt = 0.0;

        if(b2_std_coll_volatility_amt is None):
            b2_std_coll_volatility_amt = 0.0;
        
        if(b2_std_coll_mty_mismatch_amt is None):
            b2_std_coll_mty_mismatch_amt = 0.0;

        if(b2_irb_colltrl_amt_ttc is None):
            b2_irb_colltrl_amt_ttc = 0.0;

        if(b2_irb_garntee_amt_ttc is None):
            b2_irb_garntee_amt_ttc = 0.0;

        if(sec_discnt_b2_irb_coll_amt_ttc is None):
            sec_discnt_b2_irb_coll_amt_ttc = 0.0;

        if(sec_discnt_b2_irb_gtee_amt_ttc is None):
            sec_discnt_b2_irb_gtee_amt_ttc = 0.0;
    
        if (reporting_reg_code is None):
            reporting_reg_code == "";

        new_row = Row(REPORTING_REGULATOR_CODE=reporting_reg_code, YEAR_MONTH=year_month,
                    DEAL_ID=deal_id, MITIGANT_ID = mitigant_id,
                    B2_STD_CRM_TOT_OUTFLOW_RWA_INDV=b2_std_crm_tot_outflow_rwa_indv,
                    B2_STD_CRM_TOT_OUTFLOW_INDV=b2_std_crm_tot_outflow_indv,
                    B2_STD_GTEE_CDS_RWA_INDV=b2_std_gtee_cds_rwa_indv,
                    B2_IRB_CRM_UFCP_GUAR_AMT_INDV = b2_irb_crm_ufcp_guar_amt,
                    B2_IRB_CRM_UFCP_CRED_DERIV_AMT_INDV = b2_irb_crm_ufcp_cred_deriv_amt,
                    CRM_UFCP_CRED_DERIV_AMT_INDV=crm_ufcp_cred_deriv_amt_indv,
                    CRM_UFCP_GUAR_AMT_INDV=crm_ufcp_guar_amt_indv,
                    CRM_FCP_ELIG_FIN_COLL_AMT_INDV=crm_fcp_elig_fin_coll_amt_indv,
                    B2_IRB_CRM_FCP_OTH_FUNDED_AMT_INDV = b2_irb_crm_fcp_oth_funded_amt,
                    B2_IRB_CRM_FCP_REAL_ESTATE_AMT=b2_irb_crm_fcp_real_estate_amt,
                    B2_IRB_CRM_FCP_OTH_PHY_COL_AMT_INDV = b2_irb_crm_fcp_oth_phy_col_amt,
                    B2_IRB_CRM_FCP_RECEIVABLES_AMT_INDV = b2_irb_crm_fcp_receivables_amt,
                    B2_STD_EFF_FIN_COLL_AMT=b2_std_eff_fin_coll_amt,
                    B2_STD_OTH_FUN_CRED_PROT_AMT_INDV = b2_std_oth_fun_cred_prot_amt,
                    B2_STD_COLL_FX_HAIRCUT_AMT_INDV=b2_std_coll_fx_haircut_amt,
                    BLANK_CRM_AMT_STD_INDV=blank_crm_amt_std,
                    BLANK_CRM_AMT_IRB_INDV=blank_crm_amt_irb,
                    B2_IRB_CRM_COD_AMT=b2_irb_crm_cod_amt,
                    B2_IRB_CRM_LIP_AMT=b2_irb_crm_lip_amt,
                    B2_STD_EFF_GTEE_AMT_INDV=b2_std_eff_gtee_amt,
                    B2_STD_EFF_CDS_AMT_INDV=b2_std_eff_cds_amt,
                    B2_STD_COLL_VOLATILITY_AMT_INDV = b2_std_coll_volatility_amt,
                    B2_STD_COLL_MTY_MISMATCH_AMT_INDV = b2_std_coll_mty_mismatch_amt,
                    B2_IRB_COLLTRL_AMT_TTC_INDV = b2_irb_colltrl_amt_ttc,
                    B2_IRB_GARNTEE_AMT_TTC_INDV = b2_irb_garntee_amt_ttc,
                    SEC_DISCNT_B2_IRB_COLL_AMT_TTC_INDV = sec_discnt_b2_irb_coll_amt_ttc,
                    SEC_DISCNT_B2_IRB_GTEE_AMT_TTC_INDV = sec_discnt_b2_irb_gtee_amt_ttc,
                    RWA_POST_CRM_AMOUNT = rwa_post_crm_amount,
                    EAD_POST_CRM_AMT = ead_post_crm_amt
                    
                    );
        returned_list.append(new_row);

    return returned_list;
#	end of enrich_corep_sub_exposure_staging()

def enrich_sub_exposures_for_mitigants(rows):
    returned_list = [];
    for current_row in rows:
        year_month = current_row[0];
        deal_id = current_row[1];
        reporting_reg_code = current_row[2];
        b2_std_crm_tot_outflow_rwa_agg = current_row[3]; 
        b2_std_crm_tot_outflow_agg = current_row[4];
        b2_std_gtee_cds_rwa_agg = current_row[5]; 
        crm_ufcp_cred_deriv_amt_agg = current_row[6]; 
        crm_ufcp_guar_amt_agg = current_row[7]; 
        crm_fcp_elig_fin_coll_amt_agg = current_row[8]; 
        b2_std_eff_gtee_amt_agg = current_row[9]; 
        b2_std_eff_cds_amt_agg = current_row[10]; 
        b2_std_coll_volatility_amt__agg = current_row[11]; 
        b2_std_coll_mty_mismatch_amt_agg = current_row[12]; 
        b2_std_eff_fin_coll_amt_agg = current_row[13]; 
        b2_std_coll_fx_haircut_amt_agg = current_row[14]; 
        b2_irb_colltrl_amt_ttc_agg = current_row[15]; 
        sec_discnt_b2_irb_coll_amt_ttc_agg = current_row[16]; 
        sec_discnt_b2_irb_gtee_amt_ttc_agg = current_row[17]; 
        blank_crm_amt_std_agg = current_row[18]; 
        blank_crm_amt_irb_agg = current_row[19]; 
        rwa_post_crm_amount = current_row[20];
        ead_post_crm_amt = current_row[21];
        corep_crm_category = current_row[22];
        double_default_flag = current_row[23];
        b2_irb_coll_amt = current_row[24];
        b2_irb_netted_coll_gbp = current_row[25];
        b2_std_oth_fun_cred_prot_amt = current_row[26];
        rwa_approach_code = current_row[27];
        b2_irb_crm_ufcp_cred_deriv_amt = current_row[28];
        b2_irb_crm_fcp_oth_funded_amt = current_row[29];
        b2_irb_crm_fcp_oth_phy_col_amt = current_row[30];
        b2_irb_crm_fcp_receivables_amt = current_row[31];
        b2_irb_crm_ufcp_guar_amt = current_row[32];
        b2_irb_garntee_amt_ttc_agg = current_row[33];
        
    
        b2_std_crm_tot_outflow_rwa =  calculate_b2_std_crm_tot_outflow_rwa(b2_std_crm_tot_outflow_rwa_agg, rwa_post_crm_amount);
        crm_fcp_real_estate_amt_indv = calculate_crm_fcp_real_estate_amt_indv(corep_crm_category, double_default_flag, b2_irb_coll_amt);
        b2_std_crm_tot_outflow = calculate_b2_std_crm_tot_outflow_fur_exp(b2_std_crm_tot_outflow_agg, ead_post_crm_amt);
        b2_irb_crm_fcp_elg_fin_col_amt = calculate_b2_irb_crm_fcp_elg_fin_col_amt(crm_fcp_elig_fin_coll_amt_agg,
                                                                                    b2_irb_netted_coll_gbp);

        b2_irb_crm_ufcp_double_def_amt = calculate_b2_irb_crm_ufcp_double_def_amt(crm_ufcp_guar_amt_agg,
                                                                                    crm_ufcp_cred_deriv_amt_agg);

        if(b2_std_gtee_cds_rwa_agg is None):
            b2_std_gtee_cds_rwa_agg = 0.0;

        new_row = Row(REPORTING_REGULATOR_CODE=reporting_reg_code, YEAR_MONTH=year_month,
                      DEAL_ID=deal_id,
                         B2_STD_CRM_TOT_OUTFLOW_RWA =  b2_std_crm_tot_outflow_rwa,
                         B2_STD_CRM_TOT_OUTFLOW = b2_std_crm_tot_outflow,
                         B2_STD_GTEE_CDS_RWA_AGG = b2_std_gtee_cds_rwa_agg ,
                         CRM_UFCP_CRED_DERIV_AMT_AGG = crm_ufcp_cred_deriv_amt_agg ,
                         CRM_UFCP_GUAR_AMT_AGG =  crm_ufcp_guar_amt_agg,
                         CRM_FCP_ELIG_FIN_COLL_AMT_AGG =  crm_fcp_elig_fin_coll_amt_agg,
                         B2_STD_EFF_GTEE_AMT_AGG =  b2_std_eff_gtee_amt_agg,
                         B2_STD_EFF_CDS_AMT_AGG = b2_std_eff_cds_amt_agg ,
                         B2_STD_COLL_VOLATILITY_AMT__AGG = b2_std_coll_volatility_amt__agg ,
                         B2_STD_COLL_MTY_MISMATCH_AMT_AGG = b2_std_coll_mty_mismatch_amt_agg ,
                         B2_STD_EFF_FIN_COLL_AMT_AGG = b2_std_eff_fin_coll_amt_agg ,
                         B2_STD_COLL_FX_HAIRCUT_AMT_AGG = b2_std_coll_fx_haircut_amt_agg ,
                         B2_IRB_COLLTRL_AMT_TTC_AGG = b2_irb_colltrl_amt_ttc_agg ,
                         SEC_DISCNT_B2_IRB_COLL_AMT_TTC_AGG = sec_discnt_b2_irb_coll_amt_ttc_agg ,
                         SEC_DISCNT_B2_IRB_GTEE_AMT_TTC_AGG = sec_discnt_b2_irb_gtee_amt_ttc_agg ,
                         BLANK_CRM_AMT_STD_AGG = blank_crm_amt_std_agg ,
                         BLANK_CRM_AMT_IRB_AGG = blank_crm_amt_irb_agg ,
                         CRM_FCP_REAL_ESTATE_AMT_INDV = crm_fcp_real_estate_amt_indv,
                         RWA_POST_CRM_AMOUNT = rwa_post_crm_amount,
                         EAD_POST_CRM_AMT = ead_post_crm_amt,
                         B2_IRB_CRM_FCP_ELG_FIN_COL_AMT = b2_irb_crm_fcp_elg_fin_col_amt,
                         B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT = b2_irb_crm_ufcp_double_def_amt,
                         B2_STD_OTH_FUN_CRED_PROT_AMT = b2_std_oth_fun_cred_prot_amt,
                         RWA_APPROACH_CODE = rwa_approach_code,
                         B2_IRB_CRM_UFCP_CRED_DERIV_AMT = b2_irb_crm_ufcp_cred_deriv_amt,
                         B2_IRB_CRM_FCP_OTH_FUNDED_AMT = b2_irb_crm_fcp_oth_funded_amt,
                         B2_IRB_CRM_FCP_OTH_PHY_COL_AMT = b2_irb_crm_fcp_oth_phy_col_amt,
                         B2_IRB_CRM_FCP_RECEIVABLES_AMT = b2_irb_crm_fcp_receivables_amt,
                         B2_IRB_CRM_UFCP_GUAR_AMT = b2_irb_crm_ufcp_guar_amt,
                         B2_IRB_GARNTEE_AMT_TTC_AGG = b2_irb_garntee_amt_ttc_agg
                         );
        returned_list.append(new_row);

    return returned_list;
#end of enrich_sub_exposures_for_mitigants()


def further_enrich_corep_exposures(sub_exp_value_pairs_boradcast_obj):
    sub_expo = list(sub_exp_value_pairs_boradcast_obj.toLocalIterator());

    sub_expo_dict = {};

    for m in sub_expo:
        sub_expo_dict[m[0]] = m[1];

    def iterateOverRows(rows):
        returned_list = [];
        for x in rows:
            current_row = x;
            reporting_reg_code = current_row[0];
            ead_post_crm_amt = current_row[1];
            year_month = current_row[2];
            deal_id = current_row[3];
            b2_std_ead_post_crm_amt_ofbal = current_row[4];
            corep_product_category = current_row[5];
            b2_std_ccf_exp = current_row[6];
            default_fund_contrib_indicator = current_row[7];
            b2_std_ead_post_crm_amt_onbal = current_row[8];
            crd4_rep_std_flag = current_row[9];
            retail_off_bal_sheet_flag = current_row[10];
            aclm_product_type = current_row[11];
            std_orig_exp_pre_conv_onbal = current_row[12];
            rwa_post_crm_amount = current_row[13];
            crr2_501a_discount_flag = current_row[14];
            v_infra_discount_factor = current_row[15];
            b3_sme_discount_flag = current_row[16];
            sme_discount_factor = current_row[17];
            b2_app_rwa_post_crm_amt = current_row[18];
            b2_std_rwa_post_crm_amt_onbal = current_row[19];
            citizens_override_flag = current_row[20];
            b2_std_rwa_post_crm_amt = current_row[21];
            ret_rwa_post_crm_amt_pct = current_row[22];
            rwa_approach_code = current_row[23];
            std_risk_weight_ratio = current_row[24];
            b2_irb_netted_coll_gbp = current_row[25];

            if (reporting_reg_code is None):
                reporting_reg_code == "";

            data = sub_expo_dict.get(deal_id);

            #b2_std_crm_tot_outflow_exp = calculate_b2_std_crm_tot_outflow_fur_exp(data, ead_post_crm_amt_exp);

            b2_std_exposure_value_ofbal = calculate_b2_std_exposure_value_ofbal_exp(b2_std_ead_post_crm_amt_ofbal,
                                                                                    corep_product_category,
                                                                                    data);

            b2_std_fully_adj_exp_ccf_50 = calculate_b2_std_fully_adj_exp_ccf_50_exp(b2_std_ccf_exp,
                                                                                    b2_std_ead_post_crm_amt_ofbal,
                                                                                    corep_product_category,
                                                                                    data,
                                                                                    default_fund_contrib_indicator);

            b2_std_fully_adj_exp_ccf_20 = calculate_b2_std_fully_adj_exp_ccf_20_exp(b2_std_ccf_exp,
                                                                                    b2_std_ead_post_crm_amt_ofbal,
                                                                                    corep_product_category,
                                                                                    data,
                                                                                    default_fund_contrib_indicator);

            b2_std_exposure_value = calculate_b2_std_exposure_value_exp(ead_post_crm_amt,
                                                                        data);

            b2_std_exposure_value_onbal_expone = calculate_b2_std_exposure_value_onbal_expone(b2_std_ead_post_crm_amt_onbal,
                                                                                corep_product_category,
                                                                                data);

            b2_std_exposure_value_onbal = calculate_b2_std_exposure_value_onbal_exp(crd4_rep_std_flag,
                                                                                    retail_off_bal_sheet_flag,
                                                                                    corep_product_category,
                                                                                    b2_std_exposure_value,
                                                                                    b2_std_exposure_value_ofbal,
                                                                                    b2_std_exposure_value_onbal_expone);

            b2_std_eff_fin_coll_amt_final = calculate_b2_std_eff_fin_coll_amt_final(data, reporting_reg_code,
                                                                                    deal_id,
                                                                                    aclm_product_type,
                                                                                    std_orig_exp_pre_conv_onbal,
                                                                                    b2_std_exposure_value_onbal);

            b2_std_crm_tot_outflow_onbal = calculate_b2_std_crm_tot_outflow_onbal(corep_product_category,
                                                                                data);

            b2_std_crm_tot_outflow_ofbal = calculate_b2_std_crm_tot_outflow_ofbal(corep_product_category,
                                                                                b2_std_ccf_exp,
                                                                                data);

            b2_std_rwa_amt_expone = calculate_b2_std_rwa_amt_expone(rwa_post_crm_amount, data);

            b2_std_rwa_amt_stg_three = calculate_b2_std_rwa_amt_stg_three(b2_app_rwa_post_crm_amt, data);

            b2_std_rwa_pre_sprt_amt = calculate_b2_std_rwa_pre_sprt_amt_exp(crr2_501a_discount_flag,
                                                                            b2_std_rwa_amt_stg_three,
                                                                            sme_discount_factor,
                                                                            v_infra_discount_factor,
                                                                            b3_sme_discount_flag);

            b2_std_rwa_amt_onbal_fur_exp = calculate_b2_std_rwa_amt_onbal_exp(b2_std_rwa_post_crm_amt_onbal,
                                                                    corep_product_category, data);

            b2_std_rwa_amt = calculate_b2_std_rwa_amt_exp(b2_app_rwa_post_crm_amt, data);

            b2_std_rwa_post_crm_amt_ofbal = calculate_b2_std_rwa_post_crm_amt_ofbal(corep_product_category,
                                                                                    retail_off_bal_sheet_flag,
                                                                                    citizens_override_flag,
                                                                                    b2_std_rwa_post_crm_amt,
                                                                                    b2_app_rwa_post_crm_amt,
                                                                                    ret_rwa_post_crm_amt_pct);

            b2_std_rwa_amt_ofbal = calculate_b2_std_rwa_amt_ofbal_fur_exp(b2_std_rwa_post_crm_amt_ofbal,
                                                                    corep_product_category,
                                                                    data);

            b2_std_rwa_amt_onbal_final = calculate_b2_std_rwa_amt_onbal_final(crd4_rep_std_flag,
                                                                            retail_off_bal_sheet_flag,
                                                                            corep_product_category,
                                                                            b2_std_rwa_amt,
                                                                            b2_std_rwa_amt_ofbal,
                                                                            b2_std_rwa_amt_onbal_fur_exp);

            b2_std_rwa_pre_sprt_amt_onbal = calculate_b2_std_rwa_pre_sprt_amt_onbal_exp(crr2_501a_discount_flag,
                                                                                        b2_std_rwa_amt_onbal_final,
                                                                                        b3_sme_discount_flag,
                                                                                        sme_discount_factor,
                                                                                        v_infra_discount_factor);

            b2_std_rwa_pre_sprt_amt_ofbal = calculate_b2_std_rwa_pre_sprt_amt_ofbal_exp(crr2_501a_discount_flag,
                                                                                        b2_std_rwa_amt_ofbal,
                                                                                        b3_sme_discount_flag,
                                                                                        sme_discount_factor,
                                                                                        v_infra_discount_factor);

            std_act_risk_weight = calculate_std_act_risk_weight_exp(b2_std_rwa_amt_stg_three, b2_std_exposure_value,
                                                                    rwa_approach_code, crr2_501a_discount_flag,
                                                                    b3_sme_discount_flag, sme_discount_factor,
                                                                    v_infra_discount_factor, std_risk_weight_ratio);

            b2_std_crm_tot_outflow_final = calculate_b2_std_crm_tot_outflow_final(corep_product_category,
                                                                                b2_std_crm_tot_outflow_onbal,
                                                                                data,
                                                                                b2_std_crm_tot_outflow_ofbal);

            b2_std_eff_cds_amt_final = calculate_b2_std_eff_cds_amt_final(corep_product_category,
                                                                        b2_std_crm_tot_outflow_onbal,
                                                                        b2_std_crm_tot_outflow_ofbal,
                                                                        data);

            b2_std_eff_gtee_amt_final = calculate_b2_std_eff_gtee_amt_final(corep_product_category,
                                                                            b2_std_crm_tot_outflow_onbal,
                                                                            b2_std_crm_tot_outflow_ofbal,
                                                                            data);
            
            b2_std_fully_adj_exp_ccf_100 = calculate_b2_std_fully_adj_exp_ccf_100_exp(b2_std_ccf_exp,
                                                                                    default_fund_contrib_indicator,
                                                                                    b2_std_ead_post_crm_amt_ofbal,
                                                                                    corep_product_category,
                                                                                    data);
            if (data is None):
                 b2_irb_crm_fcp_elg_fin_col_amt =  b2_irb_netted_coll_gbp
            else:
                b2_irb_crm_fcp_elg_fin_col_amt =  data[6];
            

            new_row = Row(REPORTING_REGULATOR_CODE=reporting_reg_code, deal_id=deal_id, year_month=year_month,
                        #b2_std_crm_tot_outflow_exp=b2_std_crm_tot_outflow_exp,
                        B2_STD_EXPOSURE_VALUE_OFBAL=b2_std_exposure_value_ofbal,
                        B2_STD_FULLY_ADJ_EXP_CCF_50=b2_std_fully_adj_exp_ccf_50,
                        B2_STD_FULLY_ADJ_EXP_CCF_20=b2_std_fully_adj_exp_ccf_20,
                        B2_STD_EXPOSURE_VALUE=b2_std_exposure_value,
                        B2_STD_EXPOSURE_VALUE_ONBAL_EXPONE=b2_std_exposure_value_onbal_expone,
                        B2_STD_EXPOSURE_VALUE_ONBAL=b2_std_exposure_value_onbal,
                        B2_STD_EFF_FIN_COLL_AMT_FINAL=b2_std_eff_fin_coll_amt_final,
                        B2_STD_CRM_TOT_OUTFLOW_ONBAL=b2_std_crm_tot_outflow_onbal,
                        B2_STD_CRM_TOT_OUTFLOW_OFBAL=b2_std_crm_tot_outflow_ofbal,
                        B2_STD_RWA_AMT_EXPONE=b2_std_rwa_amt_expone,                          
                        B2_STD_RWA_PRE_SPRT_AMT=b2_std_rwa_pre_sprt_amt,                                        
                        B2_STD_RWA_POST_CRM_AMT_OFBAL=b2_std_rwa_post_crm_amt_ofbal,                          
                        B2_STD_RWA_AMT_ONBAL_FINAL=b2_std_rwa_amt_onbal_final,
                        B2_STD_RWA_PRE_SPRT_AMT_ONBAL=b2_std_rwa_pre_sprt_amt_onbal,
                        B2_STD_RWA_PRE_SPRT_AMT_OFBAL=b2_std_rwa_pre_sprt_amt_ofbal,
                        STD_ACT_RISK_WEIGHT=std_act_risk_weight,
                        B2_STD_CRM_TOT_OUTFLOW_FINAL=b2_std_crm_tot_outflow_final,
                        B2_STD_EFF_CDS_AMT_FINAL=b2_std_eff_cds_amt_final,
                        B2_STD_EFF_GTEE_AMT_FINAL=b2_std_eff_gtee_amt_final,
                        B2_STD_FULLY_ADJ_EXP_CCF_100=b2_std_fully_adj_exp_ccf_100,
                        B2_STD_RWA_AMT_OFBAL = b2_std_rwa_amt_ofbal,
                        B2_STD_RWA_AMT = b2_std_rwa_amt,
                        B2_IRB_CRM_FCP_ELG_FIN_COL_AMT = b2_irb_crm_fcp_elg_fin_col_amt
                        );
            returned_list.append(new_row);
        return returned_list;

    return iterateOverRows;
#	end of further_enrich_corep_exposures()


def further_enrich_sub_exposure(exposures_value_pair_boradcast_obj):
    further_enrich_subexp = list(exposures_value_pair_boradcast_obj.toLocalIterator());

    further_enrich_subexp_dict = {};

    for m in further_enrich_subexp:
        further_enrich_subexp_dict[m[0]] = m[1];

    def iterateOverRows(rows):
        
        returned_list = [];
        for x in rows:
            current_row = x;
            reporting_reg_code = current_row[0];
            deal_id = current_row[1];
            year_month = current_row[2];
            b2_std_eff_gtee_amt = current_row[3];
            b2_std_eff_cds_amt = current_row[4];
            b2_std_gtee_cds_rwa = current_row[5];
            b2_std_gtee_cds_rwa_agg = current_row[6];
            risk_taker_rru = -9999999999;
            risk_on_rru = -9999999999;
            internal_transaction_flag = '';
            ig_exclusion_flag = '';
            risk_taker_gs_code = '';
            risk_on_gs_code = '';
            default_fund_contrib_indicator = '';
            basel_data_flag = '';
            aclm_product_type = '';
            division = '';
            b2_std_crm_tot_outflow = current_row[7];
            b2_std_eff_gtee_amt_input = current_row[8];
            b2_std_eff_cds_amt_input = current_row[9];
            mitigant_id = current_row[10];
            b2_std_gtor_rw_ratio = current_row[11];

            if (reporting_reg_code is None):
                reporting_reg_code == "";

            data = further_enrich_subexp_dict.get(deal_id);

            b2_std_exposure_value_ofbal = calculate_b2_std_exposure_value_ofbal_subexp(b2_std_eff_gtee_amt, b2_std_eff_cds_amt,b2_std_crm_tot_outflow,data);

            b2_std_fully_adj_exp_ccf_50 = calculate_b2_std_fully_adj_exp_ccf_50_subexp(data,
                                                                                    b2_std_exposure_value_ofbal);

            b2_std_fully_adj_exp_ccf_20 = calculate_b2_std_fully_adj_exp_ccf_20_subexp(data,
                                                                                    b2_std_exposure_value_ofbal);

            # b2_std_eff_gtee_amt & b2_std_eff_cds_amt - coming from int_agg_mittigant layer
            #b2_std_eff_gtee_amt_input & b2_std_eff_cds_amt_input - direct pass through coming from interface
            #@VIJAY            
            b2_std_exposure_value = calculate_b2_std_exposure_value_subexp(data, b2_std_crm_tot_outflow,


                                                                            b2_std_eff_gtee_amt,b2_std_eff_cds_amt,
                                                                            b2_std_eff_gtee_amt_input,
                                                                            b2_std_eff_cds_amt_input);
            b2_std_exposure_value_onbal = calculate_b2_std_exposure_value_onbal_subexp(data,b2_std_crm_tot_outflow,b2_std_eff_gtee_amt,
                                                                                    b2_std_eff_cds_amt);

            b2_std_crm_tot_inflow = calculate_b2_std_crm_tot_inflow(data, b2_std_exposure_value_onbal,
                                                                    b2_std_exposure_value_ofbal, b2_std_exposure_value);

            b2_std_crm_tot_inflow_onbal = calculate_b2_std_crm_tot_inflow_onbal(data, b2_std_exposure_value_onbal);

            b2_std_crm_tot_inflow_ofbal = calculate_b2_std_crm_tot_inflow_ofbal(data, b2_std_exposure_value_ofbal);

            b2_std_rwa_pre_sprt_amt = calculate_b2_std_rwa_pre_sprt_amt_subexp(data);

            b2_std_rwa_amt_onbal_subexp = calculate_b2_std_rwa_amt_onbal_subexp(data, b2_std_gtee_cds_rwa);

            b2_std_rwa_pre_sprt_amt_onbal = calculate_b2_std_rwa_pre_sprt_amt_onbal_subexp(data,
                                                                                        b2_std_rwa_amt_onbal_subexp);

            b2_std_rwa_amt_ofbal = calculate_b2_std_rwa_amt_ofbal_subexp(data,
                                                                        b2_std_gtee_cds_rwa,
                                                                        b2_std_gtee_cds_rwa_agg);

            b2_std_rwa_pre_sprt_amt_ofbal = calculate_b2_std_rwa_pre_sprt_amt_ofbal_subexp(data,
                                                                                        b2_std_rwa_amt_ofbal);

            std_act_risk_weight = calculate_std_act_risk_weight_subexp(data,b2_std_gtor_rw_ratio, b2_std_exposure_value);

            b2_std_fully_adj_exp_ccf_100 = calculate_b2_std_fully_adj_exp_ccf_100_subexp(data,
                                                                                        b2_std_exposure_value_ofbal);

            if (data is not None and len(data) != 0):
                if(data[11] is not None):
                    risk_taker_rru = data[11]
                else:
                    risk_taker_rru = -9999999999
                
                if(data[12] is not None):
                    risk_on_rru = data[12]
                else:
                    risk_on_rru = -9999999999
                
                if(data[13] is not None):
                    internal_transaction_flag = data[13]
                else:
                    internal_transaction_flag = ''
                
                if(data[14] is not None):
                    ig_exclusion_flag = data[14]
                else:
                    ig_exclusion_flag = ''

                if(data[15] is not None):
                    risk_taker_gs_code = data[15]
                else:
                    risk_taker_gs_code = ''
                
                if(data[16] is not None):
                    risk_on_gs_code = data[16]
                else:
                    risk_on_gs_code = ''
                
                if(data[17] is not None):
                    default_fund_contrib_indicator = data[17]
                else:
                    default_fund_contrib_indicator = ''

                if(data[18] is not None):
                    basel_data_flag = data[18]
                else:
                    basel_data_flag = ''
                
                if(data[19] is not None):
                    aclm_product_type = data[19]
                else:
                    aclm_product_type = ''

                if(data[20] is not None):
                    division = data[20]
                else:
                    division = ''                                        

            new_row = Row(
                REPORTING_REGULATOR_CODE=reporting_reg_code,
                deal_id=deal_id,
                year_month=year_month,
                B2_STD_EXPOSURE_VALUE_OFBAL=b2_std_exposure_value_ofbal,
                B2_STD_FULLY_ADJ_EXP_CCF_50=b2_std_fully_adj_exp_ccf_50,
                B2_STD_FULLY_ADJ_EXP_CCF_20=b2_std_fully_adj_exp_ccf_20,                
                B2_STD_EXPOSURE_VALUE_ONBAL=b2_std_exposure_value_onbal,
                B2_STD_CRM_TOT_INFLOW=b2_std_crm_tot_inflow,
                B2_STD_CRM_TOT_INFLOW_ONBAL=b2_std_crm_tot_inflow_onbal,
                B2_STD_CRM_TOT_INFLOW_OFBAL=b2_std_crm_tot_inflow_ofbal,
                B2_STD_RWA_PRE_SPRT_AMT=b2_std_rwa_pre_sprt_amt,
                B2_STD_RWA_PRE_SPRT_AMT_ONBAL=b2_std_rwa_pre_sprt_amt_onbal,
                B2_STD_RWA_PRE_SPRT_AMT_OFBAL=b2_std_rwa_pre_sprt_amt_ofbal,
                STD_ACT_RISK_WEIGHT=std_act_risk_weight,
                B2_STD_FULLY_ADJ_EXP_CCF_100=b2_std_fully_adj_exp_ccf_100,
                RISK_TAKER_RRU = risk_taker_rru,
                RISK_ON_RRU = risk_on_rru,
                INTERNAL_TRANSACTION_FLAG = internal_transaction_flag,
                IG_EXCLUSION_FLAG = ig_exclusion_flag,
                RISK_TAKER_GS_CODE = risk_taker_gs_code,
                RISK_ON_GS_CODE = risk_on_gs_code,
                DEFAULT_FUND_CONTRIB_INDICATOR = default_fund_contrib_indicator,
                BASEL_DATA_FLAG = basel_data_flag,
                ACLM_PRODUCT_TYPE = aclm_product_type,
                DIVISION = division,
                B2_STD_RWA_AMT_ONBAL_FINAL = b2_std_rwa_amt_onbal_subexp,
                B2_STD_EXPOSURE_VALUE = b2_std_exposure_value,
                MITIGANT_ID = mitigant_id
            );
            returned_list.append(new_row);
        return returned_list;

    return iterateOverRows;
#	end of further_enrich_sub_exposure()


def enrich_crd4_df(rows):
    returned_list = [];
    for current_row in rows:
#         try:
            reporting_reg_code = current_row[0];
            deal_id = current_row[1];
            year_month = current_row[2];
            ead_post_crm_amt = current_row[3];
            b2_std_crm_tot_outflow = current_row[4];
            b2_app_rwa_post_crm_amt = current_row[5];
            b2_std_crm_tot_outflow_rwa = current_row[6];
            std_risk_weight_ratio = current_row[7];
            rwa_approach_code = current_row[8];
            crr2_501a_discount_flag = current_row[9];
            b3_sme_discount_flag = current_row[10];
            sme_discount_factor = current_row[11];
            aclm_product_type = current_row[12];
            b2_irb_drawn_rwa = current_row[13];
            retail_off_bal_sheet_flag = current_row[14];
            rwa_post_crm_amt = current_row[15];
            b2_std_gtee_cds_rwa = current_row[16];
            corep_product_category = current_row[17];
            b2_std_gtee_cds_rwa_agg = current_row[18];
            b2_std_rwa_post_crm_amt_onbal = current_row[19];
            v_infra_discount_factor = current_row[20];
            approved_approach_code = current_row[21];
            b2_app_ead_post_crm_amt = current_row[22];
            regulatory_product_type = current_row[23];
            corep_exposure_category = current_row[24];
            irb_orig_exp_pre_conv_ofbal = current_row[25];
            b2_irb_orig_exp_pre_con_factor = current_row[26];
            sme_discount_factor = current_row[27];
            b2_irb_rwa_amt_ofbal = current_row[28];
            b2_std_asset_class_basel_exposure_type = current_row[29];
            sub_sector = current_row[30];
            b2_irb_asset_class_reg_exposure_type = current_row[31];           
            flow_type = current_row[32];


#            b2_std_rwa_amt = calculate_b2_std_rwa_amt(b2_app_rwa_post_crm_amt, b2_std_crm_tot_outflow_rwa);

#            b2_std_rwa_amt_ofbal = calculate_b2_std_rwa_amt_ofbal(corep_product_category, rwa_post_crm_amt,
#                                                                  b2_std_gtee_cds_rwa, b2_std_gtee_cds_rwa_agg);

            b2_irb_rwa_amt_onbal = calculate_b2_irb_rwa_amt_onbal(aclm_product_type, b2_irb_drawn_rwa,
                                                                retail_off_bal_sheet_flag, b2_app_rwa_post_crm_amt);

            b2_std_rwa_amt_onbal = calculate_b2_std_rwa_amt_onbal(b2_std_rwa_post_crm_amt_onbal, corep_product_category,
                                                                b2_std_crm_tot_outflow_rwa);

            irb_risk_weight_ratio = float(calculate_irb_risk_weight_ratio(approved_approach_code, b2_app_ead_post_crm_amt,
                                                                    crr2_501a_discount_flag, b3_sme_discount_flag,
                                                                    v_infra_discount_factor,
                                                                    sme_discount_factor, b2_app_rwa_post_crm_amt));
            

            irb_orig_exp_pre_conv_onbal = calculate_irb_orig_exp_pre_conv_onbal(corep_exposure_category,
                                                                                irb_orig_exp_pre_conv_ofbal,
                                                                                aclm_product_type,
                                                                                b2_irb_orig_exp_pre_con_factor);

            rwa_post_crm_pre_sprt_amount = calculate_rwa_post_crm_pre_sprt_amount(crr2_501a_discount_flag,
                                                                                rwa_post_crm_amt,
                                                                                v_infra_discount_factor,
                                                                                b3_sme_discount_flag,
                                                                                sme_discount_factor);

            b2_irb_rwa_pre_sprt_amt_onbal = calculate_b2_irb_rwa_pre_sprt_amt_onbal(crr2_501a_discount_flag,
                                                                                    b2_irb_rwa_amt_onbal,
                                                                                    v_infra_discount_factor,
                                                                                    b3_sme_discount_flag,
                                                                                    sme_discount_factor);

            b2_irb_rwa_pre_sprt_amt_ofbal = calculate_b2_irb_rwa_pre_sprt_amt_ofbal(crr2_501a_discount_flag,
                                                                                    b2_irb_rwa_amt_ofbal,
                                                                                    v_infra_discount_factor,
                                                                                    b3_sme_discount_flag,
                                                                                    sme_discount_factor);

            finrep_counterparty_sector = calculate_finrep_counterparty_sector(rwa_approach_code,
                                                                            b2_std_asset_class_basel_exposure_type,
                                                                            sub_sector,
                                                                            b2_irb_asset_class_reg_exposure_type)

            #b2_irb_crm_fcp_elg_fin_col_amt = calculate_b2_irb_crm_fcp_elg_fin_col_amt(crm_fcp_elig_fin_coll_amt,
             #                                                                       b2_irb_netted_coll_gbp);

            #b2_irb_crm_ufcp_double_def_amt = calculate_b2_irb_crm_ufcp_double_def_amt(crm_ufcp_guar_amt,
             #                                                                       crm_ufcp_cred_deriv_amt);

            if (reporting_reg_code is None):
                reporting_reg_code == "";

            new_row = Row(REPORTING_REGULATOR_CODE=reporting_reg_code, YEAR_MONTH=year_month,
                        DEAL_ID=deal_id,
#                         B2_STD_RWA_AMT=b2_std_rwa_amt,
#                         B2_STD_RWA_AMT_OFBAL=b2_std_rwa_amt_ofbal,
                        B2_IRB_RWA_AMT_ONBAL=b2_irb_rwa_amt_onbal,
                        B2_STD_RWA_AMT_ONBAL=b2_std_rwa_amt_onbal,
                        IRB_RISK_WEIGHT_RATIO=irb_risk_weight_ratio,                          
                        IRB_ORIG_EXP_PRE_CONV_ONBAL=irb_orig_exp_pre_conv_onbal,
                        RWA_POST_CRM_PRE_SPRT_AMOUNT=rwa_post_crm_pre_sprt_amount,
                        B2_IRB_RWA_PRE_SPRT_AMT_ONBAL=b2_irb_rwa_pre_sprt_amt_onbal,
                        B2_IRB_RWA_PRE_SPRT_AMT_OFBAL=b2_irb_rwa_pre_sprt_amt_ofbal,
                        FINREP_COUNTERPARTY_SECTOR=finrep_counterparty_sector,
                        #B2_IRB_CRM_FCP_ELG_FIN_COL_AMT=b2_irb_crm_fcp_elg_fin_col_amt,
                        #B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT=b2_irb_crm_ufcp_double_def_amt,
                        FLOW_TYPE = flow_type
                        );
            returned_list.append(new_row);
#         except Exception as e:
#             print("\nException occurred in inner loop of enrich_crd4_df : ", e);

    return returned_list;
#	end of enrich_crd4_df()


def join_exp_and_sub_exp(exp_data, sub_exp_data):
    select_query = "select b.DEAL_ID, a.VALID_FLAG, a.MITIGANT_ID, a.SYS_CODE, a.ADJUST_FLAG, \
                    a.COREP_CRM_CATEGORY, a.B2_IRB_COLL_AMT, a.B2_IRB_GTEE_AMT, a.B2_STD_EFF_GTEE_AMT, \
                    a.B2_STD_EFF_CDS_AMT, a.B2_STD_ELIG_COLL_AMT, a.B2_STD_EFF_COLL_AMT_POST_HCUT, \
                    a.B2_STD_COLL_VOLATILITY_AMT, a.B2_STD_COLL_MTY_MISMATCH_AMT, a.B2_STD_GTOR_RW_RATIO, \
                    a.B2_STD_GTEE_CDS_RWA, a.B2_STD_FX_HAIRCUT, a.TYPE, a.B2_IRB_COLLTRL_AMT_TTC, \
                    a.B2_IRB_GARNTEE_AMT_TTC, a.SEC_DISCNT_B2_IRB_COLL_AMT_TTC, a.SEC_DISCNT_B2_IRB_GTEE_AMT_TTC, \
                    a.DOUBLE_DEFAULT_FLAG, a.SEC_DISCNTD_B2_IRB_GARNTEE_AMT, a.SEC_DISCNTD_B2_IRB_COLLTRL_AMT, \
                    a.REPORTING_REGULATOR_CODE, a.SYS_PROTECTION_TYPE, a.BASEL_SECURITY_SUB_TYPE, \
                    a.PROVIDER_B2_STD_ASSET_CLASS, a.MITIGANT_TYPE, a.COLLATERAL_TYPE_CODE, \
                    a.FLOW_TYPE, a.COREP_STD_ASSET_CLASS_RK, a.YEAR_MONTH, b.RWA_POST_CRM_AMOUNT, \
                    b.EAD_POST_CRM_AMT, b.B2_IRB_NETTED_COLL_GBP, b.RWA_APPROACH_CODE";

    sub_exp_data.createOrReplaceTempView("corep_sub_exp_stg_data");
    exp_data.createOrReplaceTempView("exposure_data");

    final_query = select_query + " from corep_sub_exp_stg_data a  , exposure_data b \
                                    where a.deal_id = substring(b.deal_id, NVL (LENGTH (b.DEAL_ID_PREFIX), 0) \
                                    + 1, (LENGTH (b.DEAL_ID) - NVL(LENGTH (b.DEAL_ID_PREFIX),0))) \
                                    and a.reporting_regulator_code = b.reporting_regulator_code and a.year_month = b.year_month"

    final_df = spark.sql(final_query);

    return final_df;
#   end of join_exp_and_sub_exp()


def get_sub_exp_cols_in_exp(further_enriched_final_exposures, agg_mitigant_df):
    #sub_exp_not_std = further_enriched_sub_exposures.filter("RWA_APPROACH_CODE not in ('STD')");
    required_sub_exp_cols = ["DEAL_ID", "REPORTING_REGULATOR_CODE", "YEAR_MONTH", "B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT",
                                "B2_IRB_CRM_UFCP_CRED_DERIV_AMT", "B2_IRB_CRM_FCP_REAL_ESTATE_AMT", 
                                #"B2_IRB_CRM_FCP_ELG_FIN_COL_AMT", 
                                "B2_IRB_COLLTRL_AMT_TTC_AGG", "B2_IRB_CRM_FCP_OTH_FUNDED_AMT",
                                "B2_IRB_CRM_FCP_OTH_PHY_COL_AMT", "B2_IRB_CRM_FCP_RECEIVABLES_AMT", "B2_IRB_CRM_UFCP_GUAR_AMT"];
    sub_exp_df = agg_mitigant_df.select(required_sub_exp_cols);
    
    further_enriched_final_exposures = further_enriched_final_exposures.join(sub_exp_df, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", "DEAL_ID"],
                                                                                how = "leftouter");

    further_enriched_final_exposures = further_enriched_final_exposures.fillna({"B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT": 0.0, "B2_IRB_CRM_UFCP_CRED_DERIV_AMT": 0.0,
                                                                                "B2_IRB_CRM_FCP_REAL_ESTATE_AMT": 0.0, 
                                                                                #"B2_IRB_CRM_FCP_ELG_FIN_COL_AMT": 0.0,
                                                                                "B2_IRB_CRM_FCP_OTH_FUNDED_AMT" : 0.0, "B2_IRB_CRM_FCP_OTH_PHY_COL_AMT": 0.0,
                                                                                "B2_IRB_CRM_FCP_RECEIVABLES_AMT": 0.0, "B2_IRB_CRM_UFCP_GUAR_AMT" : 0.0,
                                                                                "B2_IRB_COLLTRL_AMT_TTC_AGG": 0.0});
    return further_enriched_final_exposures;
#   end of get_sub_exp_cols_in_exp()


def get_exp_cols_in_sub_exp(further_enriched_final_exposures, further_enriched_sub_exposures):
    required_exp_cols = ["DEAL_ID", "REPORTING_REGULATOR_CODE", "YEAR_MONTH", "B3_SME_RP_FLAG",
                                "RETAIL_POOL_SME_FLAG", "COREP_PRODUCT_CATEGORY", "External_Counterparty_Id",
                                    "REGULATORY_PRODUCT_TYPE", "INDUSTRY_SECTOR_TYPE", "SOVEREIGN_REGIONAL_GOVT_FLAG",
                                    "B2_STD_CCF_INPUT","B2_IRB_CCF", "SECTOR_TYPE", "COREP_EXPOSURE_CATEGORY",
                                    "B2_APP_ADJUSTED_PD_RATIO","COREP_IRB_ASSET_CLASS_RK"];

    #further_enriched_final_exposures = further_enriched_final_exposures.fillna({"ON_BALANCE_LEDGER_EXPOSURE": 0.0});

    exp_df = further_enriched_final_exposures.select(required_exp_cols);
    exp_df = exp_df.withColumnRenamed("B2_STD_CCF_INPUT","B2_STD_CCF");
    
    further_enriched_sub_exposures = further_enriched_sub_exposures.join(exp_df, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", "DEAL_ID"], how = "leftouter");

    further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn("ON_BALANCE_LEDGER_EXPOSURE", lit(0));
    further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn("B2_IRB_PROVISIONS_AMT", lit(0));
    further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn("B2_STD_PROVISIONS_AMT_ONBAL", lit(0));
    further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn("CRD4_REP_STD_FLAG", lit('N').cast(StringType()));
    further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn("CRD4_REP_STDFLOW_FLAG", lit('Y').cast(StringType()));

    return further_enriched_sub_exposures;
#   end of get_exp_cols_in_sub_exp()


if __name__ == "__main__":
    spark = get_spark_session();
    config = read_config(spark);
    srtd_mart_db = config["Database_details"]["srtd_mart_db"];
    rtd_ref_db = config["Database_details"]["rtd_ref_db"];
	p_reporting_date = '2022-06-30'
    #p_reporting_date = config["Input_parameters"]["reporting_date"];
    reporting_date = datetime.datetime.strptime(p_reporting_date, '%Y-%m-%dT%H::%M::%S.%f').strftime('%d-%m-%Y')
    year_month = datetime.datetime.strptime(p_reporting_date,'%Y-%m-%dT%H::%M::%S.%f').strftime('%Y%m');
    
    print("\nTriggering transformation for reporting_date = ", reporting_date);
    print("\nTriggering transformation for year_month = ", year_month);

    ###############################################################
    #########--Create Broadcast Variables Previous Quarter-########
    ###############################################################
    crd4_prev_qtr_date = get_prev_qtr_date(reporting_date, rtd_ref_db)
    prev_qarter_value_pairs = get_prev_qtr_df(crd4_prev_qtr_date, srtd_mart_db)
    prev_qarter_value_boradcast_obj = broadcast(prev_qarter_value_pairs);

    ###############################################################
    #########-----------Enriching Exposures-----------#############
    ###############################################################
    exp_query = "select * from " + srtd_mart_db + ".rwa_reporting_exposures_staging where year_month = " + year_month + " and valid_flag = 'Y'";
    print("\nExposure Query = ", exp_query);
    print("\n");
    corep_exp_stg_data = spark.sql(exp_query).distinct();
    corep_exp_stg_data = corep_exp_stg_data.withColumnRenamed("B2_STD_CCF","B2_STD_CCF_INPUT");
    corep_exp_stg_data = corep_exp_stg_data.persist(StorageLevel.MEMORY_AND_DISK_SER)
    print("\n\n\n\nTotal input fetched from exposures staging : ", corep_exp_stg_data.count());


    columns = ["REPORTING_REGULATOR_CODE", "PD_MODEL_CODE", "B2_IRB_ASSET_CLASS_BASEL_EXPOSURE_TYPE",
            "B2_IRB_ASSET_CLASS_REG_EXPOSURE_SUB_TYPE", "DIVISION_SUB2", "DEAL_ID",
            "APPROVED_APPROACH_CODE", "YEAR_MONTH", "B2_STD_ASSET_CLASS_BASEL_EXPOSURE_TYPE",
            "REPORTING_TYPE_CODE", "B2_STD_RISK_WEIGHT_RATIO", "BASEL_DATA_FLAG",
            "CITIZENS_OVERRIDE_FLAG", "B2_STD_EAD_POST_CRM_AMT", "B2_APP_EAD_POST_CRM_AMT",
            "EXTERNAL_COUNTERPARTY_ID_TEMP", "IFRS9_TOT_REGU_ECL_AMT_GBP", "PROVISION_AMOUNT_GBP",
            "COREP_PRODUCT_CATEGORY", "B3_CVA_PROVISION_AMT", "B2_STD_RWA_POST_CRM_AMT",
            "B2_APP_RWA_POST_CRM_AMT", "RETAIL_OFF_BAL_SHEET_FLAG", "B2_STD_DRAWN_RWA", "B2_STD_UNDRAWN_RWA",
            "ACLM_PRODUCT_TYPE", "B2_IRB_UNDRAWN_RWA", "B2_APP_ADJUSTED_PD_RATIO", "B2_APP_ADJUSTED_LGD_RATIO",
            "B2_IRB_ASSET_CLASS_REG_EXPOSURE_SUB_TYPE", "B2_IRB_ASSET_CLASS_BASEL_EXPOSURE_TYPE",
            "DEFINITIVE_SLOT_CATEGORY_CALC", "REGULATORY_PRODUCT_TYPE",
            "PD_MODEL_CODE", "B2_IRB_ASSET_CLASS_REG_EXPOSURE_TYPE", "CRR2_501A_DISCOUNT_FLAG",
            "V_INFRA_DISCOUNT_FACTOR", "B3_SME_DISCOUNT_FLAG", "SME_DISCOUNT_FACTOR",
            "APPROVED_APPROACH_CODE", "REPORTING_REGULATOR_CODE", "COREP_IRB_EXPOSURE_SUB_TYPE",
            "B2_IRB_ASSET_CLASS_BASEL_EXPOSURE_SUB_TYPE",
            "TYPE", "DAC_TYPE", "DAC_VALID_FLAG", "DAC_ASSET_CLASS_RK", "B2_IRB_DRAWN_EAD", "B2_IRB_UNDRAWN_EAD",
            "BANK_BASE_ROLE_CODE", "OFF_BAL_LED_EXP_GBP_POST_SEC", "UNDRAWN_COMMITMENT_AMT_PST_SEC",
            "OFF_BALANCE_EXPOSURE", "UNDRAWN_COMMITMENT_AMT", "B2_IRB_EFFECTIVE_MATURITY_YRS", "B2_STD_EAD_PRE_CRM_AMT",
            "B2_APP_EAD_PRE_CRM_AMT", "B2_IRB_DRAWN_EXPECTED_LOSS", "B2_APP_EXPECTED_LOSS_AMT", "FI_AVC_CATEGORY",
            "B2_IRB_UNDRAWN_EXPECTED_LOSS", "OBLIGORS_COUNT", "DIM_PD_BAND_CODE", "DIM_MGS", "RESIDUAL_MATURITY_DAYS",
            "EC92_CODE", "BSD_MARKER", "SECTOR_CLUSTER", "SUB_SECTOR", 
            "B2_STD_PRE_DFLT_ASSET_CLASS_BASEL_EXPOSURE_TYPE", "MEMO_B2_STD_BASEL_EXPOSURE_TYPE",
            "External_COUNTERPARTY_QCCP_FLAG", "B2_STD_CCF_INPUT", "GRDW_POOL_ID", "COMM_LESS_THAN_1_YEAR",
            "COMM_MORE_THAN_1_YEAR", "B2_STD_ORIG_EXP_PRE_CON_FACTOR", "CSYS_ID","CSYS_CODE", 
            "MEMO_B2_STD_BASEL_EXPOSURE_SUB_TYPE", "MEMO_B2_STD_TYPE", "MEMO_B2_STD_REG_EXPOSURE_TYPE",
            "TYPE","UNSETTLED_PRICE_DIFFERENCE_AMT", "INTERNAL_TRANSACTION_FLAG", "OVERRIDE_VALUE", "V_ICB_EFF_FROM_YEAR_MONTH_RK", 
            "RISK_ON_GS_CODE", "RISK_TAKER_GROUP_STRUCTURE", "RISK_ON_GROUP_STRUCTURE", "RISK_TAKER_GS_CODE", "DIM_CTRPTY_RISK_ENTITY_CLASS", 
            "RISK_TAKER_LL_RRU", "V_CORE_IG_EFF_YEAR_MONTH_RK", "RWA_CALC_METHOD", "AIML_PRODUCT_TYPE", "COST_CENTRE", "BSPL_900_GL_CODE", "NET_EXPOSURE", 
            "BSPL_60_GL_CODE", "DRAWN_LEDGER_BALANCE", "B2_IRB_INTEREST_AT_DEFAULT_GBP", "ACCRUED_INTEREST_GBP", "E_STAR_GROSS_EXPOSURE", "CRD4_REP_IRBEQ_REG_EXPOSURE_TYPE","OBLIGOR_TYPE"];

    corep_exposure_staging = corep_exp_stg_data.select(columns);

    enriched_exposures = corep_exposure_staging.rdd.mapPartitions(
        enrich_corep_exposure_staging(prev_qarter_value_boradcast_obj)).toDF()

    #Removing the ambiguious columns just to avoid duplicates
    corep_exp_stg_data = corep_exp_stg_data.drop('UNSETTLED_PRICE_DIFFERENCE_AMT')
    final_exposures = corep_exp_stg_data.join(enriched_exposures, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", "DEAL_ID"],
                                            how="inner").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER);
    print('\n\n\n\ntotal records in final_exposures : ', final_exposures.count());


    ###############################################################
    #########-----------Enriching Sub Exposures-----------#########
    ###############################################################
    sub_exp_query = "select * from " + srtd_mart_db + ".rwa_reporting_sub_exposures_staging where year_month = " + year_month + " and valid_flag = 'Y'";
    print("\nSub Exposure Query = ", sub_exp_query);
    print("\n");
    sub_exp_data = spark.sql(sub_exp_query).distinct();
    print('\n\nTotal records in sub_exposures_staging before join with exposures : ', sub_exp_data.count());
    print("\n\n");

    corep_sub_exp_stg_data = join_exp_and_sub_exp(final_exposures, sub_exp_data);
    corep_sub_exp_stg_data = corep_sub_exp_stg_data.persist(StorageLevel.MEMORY_AND_DISK_SER);
    print('\n\n\n\nTotal records in sub_exposures_staging after join with exposures : ', corep_sub_exp_stg_data.count());
    print("\n\n");

    print("\n\n **** Starting sub-exposures enrichment **** \n\n")    

    columns = ["REPORTING_REGULATOR_CODE", "DEAL_ID", "YEAR_MONTH",
            "B2_STD_EFF_GTEE_AMT", "B2_STD_EFF_CDS_AMT", "TYPE", "B2_STD_GTEE_CDS_RWA", "MITIGANT_TYPE",
            "COREP_CRM_CATEGORY", "SEC_DISCNTD_B2_IRB_GARNTEE_AMT", "DOUBLE_DEFAULT_FLAG", "B2_IRB_GTEE_AMT",
            "SEC_DISCNTD_B2_IRB_COLLTRL_AMT", "B2_IRB_COLL_AMT", "B2_STD_EFF_COLL_AMT_POST_HCUT",
            "B2_STD_FX_HAIRCUT", "B2_STD_ELIG_COLL_AMT", "COLLATERAL_TYPE_CODE", "SYS_PROTECTION_TYPE", "SYS_CODE",
            "MITIGANT_ID","B2_STD_COLL_VOLATILITY_AMT","B2_STD_COLL_MTY_MISMATCH_AMT","B2_IRB_COLLTRL_AMT_TTC",
            "B2_IRB_GARNTEE_AMT_TTC","SEC_DISCNT_B2_IRB_COLL_AMT_TTC","SEC_DISCNT_B2_IRB_GTEE_AMT_TTC",
            "RWA_POST_CRM_AMOUNT","EAD_POST_CRM_AMT"];

    corep_sub_exposures_staging = corep_sub_exp_stg_data.select(columns);

    enriched_sub_exposures = corep_sub_exposures_staging.rdd.mapPartitions(enrich_corep_sub_exposure_staging).toDF()

    print("\n\n\n\n **** Starting sub-query aggregations in sub-exposures **** \n\n\n\n")

    #Removing mitigant_id from group by column set
    agg_sub_exposures = enriched_sub_exposures.groupBy("YEAR_MONTH", "DEAL_ID",
                                                       "REPORTING_REGULATOR_CODE")\
                                                .agg(sum('B2_STD_CRM_TOT_OUTFLOW_RWA_INDV').alias("B2_STD_CRM_TOT_OUTFLOW_RWA_AGG"),
                                                     sum('B2_STD_CRM_TOT_OUTFLOW_INDV').alias("B2_STD_CRM_TOT_OUTFLOW_AGG"),
                                                     sum('B2_STD_GTEE_CDS_RWA_INDV').alias("B2_STD_GTEE_CDS_RWA_AGG"),
                                                     sum('CRM_UFCP_CRED_DERIV_AMT_INDV').alias("CRM_UFCP_CRED_DERIV_AMT_AGG"),
                                                     sum('CRM_UFCP_GUAR_AMT_INDV').alias("CRM_UFCP_GUAR_AMT_AGG"),
                                                     sum('CRM_FCP_ELIG_FIN_COLL_AMT_INDV').alias("CRM_FCP_ELIG_FIN_COLL_AMT_AGG"),
                                                     sum('B2_STD_EFF_GTEE_AMT_INDV').alias('B2_STD_EFF_GTEE_AMT_AGG'),
                                                     sum('B2_STD_EFF_CDS_AMT_INDV').alias('B2_STD_EFF_CDS_AMT_AGG'),
                                                     sum('B2_STD_COLL_VOLATILITY_AMT_INDV').alias('B2_STD_COLL_VOLATILITY_AMT__AGG'),
                                                     sum('B2_STD_COLL_MTY_MISMATCH_AMT_INDV').alias('B2_STD_COLL_MTY_MISMATCH_AMT_AGG'),
                                                     sum('B2_STD_EFF_FIN_COLL_AMT').alias('B2_STD_EFF_FIN_COLL_AMT_AGG'),
                                                     sum('B2_STD_COLL_FX_HAIRCUT_AMT_INDV').alias('B2_STD_COLL_FX_HAIRCUT_AMT_AGG'),
                                                     sum('B2_IRB_COLLTRL_AMT_TTC_INDV').alias('B2_IRB_COLLTRL_AMT_TTC_AGG'),
                                                     sum('SEC_DISCNT_B2_IRB_COLL_AMT_TTC_INDV').alias('SEC_DISCNT_B2_IRB_COLL_AMT_TTC_AGG'),
                                                     sum('SEC_DISCNT_B2_IRB_GTEE_AMT_TTC_INDV').alias('SEC_DISCNT_B2_IRB_GTEE_AMT_TTC_AGG'),
                                                     sum('BLANK_CRM_AMT_STD_INDV').alias('BLANK_CRM_AMT_STD_AGG'),
                                                     sum('BLANK_CRM_AMT_IRB_INDV').alias('BLANK_CRM_AMT_IRB_AGG'),
                                                     sum('B2_STD_OTH_FUN_CRED_PROT_AMT_INDV').alias('B2_STD_OTH_FUN_CRED_PROT_AMT_AGG'),
                                                     sum('B2_IRB_CRM_UFCP_CRED_DERIV_AMT_INDV').alias('B2_IRB_CRM_UFCP_CRED_DERIV_AMT_AGG'),
                                                     sum('B2_IRB_CRM_FCP_OTH_FUNDED_AMT_INDV').alias('B2_IRB_CRM_FCP_OTH_FUNDED_AMT_AGG'),
                                                     sum('B2_IRB_CRM_FCP_OTH_PHY_COL_AMT_INDV').alias('B2_IRB_CRM_FCP_OTH_PHY_COL_AMT_AGG'),
                                                     sum('B2_IRB_CRM_FCP_RECEIVABLES_AMT_INDV').alias('B2_IRB_CRM_FCP_RECEIVABLES_AMT_AGG'),
                                                     sum('B2_IRB_CRM_UFCP_GUAR_AMT_INDV').alias('B2_IRB_CRM_UFCP_GUAR_AMT_AGG'),
                                                     sum('B2_IRB_GARNTEE_AMT_TTC_INDV').alias('B2_IRB_GARNTEE_AMT_TTC_AGG')
                                                    );

    agg_sub_exposures = agg_sub_exposures.withColumnRenamed("B2_STD_OTH_FUN_CRED_PROT_AMT_AGG", "B2_STD_OTH_FUN_CRED_PROT_AMT");
    agg_sub_exposures = agg_sub_exposures.withColumnRenamed("B2_IRB_CRM_UFCP_CRED_DERIV_AMT_AGG", "B2_IRB_CRM_UFCP_CRED_DERIV_AMT");
    agg_sub_exposures = agg_sub_exposures.withColumnRenamed("B2_IRB_CRM_FCP_OTH_FUNDED_AMT_AGG", "B2_IRB_CRM_FCP_OTH_FUNDED_AMT");
    agg_sub_exposures = agg_sub_exposures.withColumnRenamed("B2_IRB_CRM_FCP_OTH_PHY_COL_AMT_AGG", "B2_IRB_CRM_FCP_OTH_PHY_COL_AMT");
    agg_sub_exposures = agg_sub_exposures.withColumnRenamed("B2_IRB_CRM_FCP_RECEIVABLES_AMT_AGG", "B2_IRB_CRM_FCP_RECEIVABLES_AMT");
    agg_sub_exposures = agg_sub_exposures.withColumnRenamed("B2_IRB_CRM_UFCP_GUAR_AMT_AGG", "B2_IRB_CRM_UFCP_GUAR_AMT");

    final_enriched_sub_exposures = corep_sub_exp_stg_data.join(agg_sub_exposures, on=["YEAR_MONTH", 
                                                                                    "REPORTING_REGULATOR_CODE", 
                                                                                    "DEAL_ID"], 
                                                            how="inner");

    print("\n\n\n\n **** Enriching sub-exposures with un-aggregated columns **** \n\n\n\n")

    columns = ["MITIGANT_ID","REPORTING_REGULATOR_CODE", "DEAL_ID", "YEAR_MONTH", "B2_IRB_CRM_COD_AMT", "B2_IRB_CRM_LIP_AMT"];
    
    corep_sub_exposures_unagg = enriched_sub_exposures.select(columns);
    #corep_sub_exposures_unagg = corep_sub_exposures_unagg.withColumnRenamed("B2_IRB_GARNTEE_AMT_TTC_INDV", "B2_IRB_GARNTEE_AMT_TTC");

    final_enriched_sub_exposures = final_enriched_sub_exposures.join(corep_sub_exposures_unagg,on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", 
                                                                                    "DEAL_ID","MITIGANT_ID"], how="inner");

    print('\n\nTotal records in sub_exposures post first level of aggregation/processing : ', final_enriched_sub_exposures.count());    
    print("\n\n");

    ############################INT_MITIGANT_LAYER##############

    print("\n\n **** Starting int-mitigant layer creation **** \n\n")

    columns = ["YEAR_MONTH", "DEAL_ID", "REPORTING_REGULATOR_CODE", "B2_STD_CRM_TOT_OUTFLOW_RWA_AGG",
            "B2_STD_CRM_TOT_OUTFLOW_AGG", "B2_STD_GTEE_CDS_RWA_AGG","CRM_UFCP_CRED_DERIV_AMT_AGG",
            "CRM_UFCP_GUAR_AMT_AGG","CRM_FCP_ELIG_FIN_COLL_AMT_AGG","B2_STD_EFF_GTEE_AMT_AGG","B2_STD_EFF_CDS_AMT_AGG",
            "B2_STD_COLL_VOLATILITY_AMT__AGG","B2_STD_COLL_MTY_MISMATCH_AMT_AGG","B2_STD_EFF_FIN_COLL_AMT_AGG",
            "B2_STD_COLL_FX_HAIRCUT_AMT_AGG","B2_IRB_COLLTRL_AMT_TTC_AGG","SEC_DISCNT_B2_IRB_COLL_AMT_TTC_AGG",
            "SEC_DISCNT_B2_IRB_GTEE_AMT_TTC_AGG","BLANK_CRM_AMT_STD_AGG","BLANK_CRM_AMT_IRB_AGG",
            "RWA_POST_CRM_AMOUNT","EAD_POST_CRM_AMT","COREP_CRM_CATEGORY","DOUBLE_DEFAULT_FLAG",
            "B2_IRB_COLL_AMT","B2_IRB_NETTED_COLL_GBP", "B2_STD_OTH_FUN_CRED_PROT_AMT", "RWA_APPROACH_CODE", "B2_IRB_CRM_UFCP_CRED_DERIV_AMT",
            "B2_IRB_CRM_FCP_OTH_FUNDED_AMT", "B2_IRB_CRM_FCP_OTH_PHY_COL_AMT", "B2_IRB_CRM_FCP_RECEIVABLES_AMT", "B2_IRB_CRM_UFCP_GUAR_AMT",
            "B2_IRB_GARNTEE_AMT_TTC_AGG"];

    int_agg_df = final_enriched_sub_exposures.select(columns);

    int_agg_mitigant_df = int_agg_df.rdd.mapPartitions(enrich_sub_exposures_for_mitigants).toDF()
    int_agg_mitigants_aggregated = int_agg_mitigant_df.groupBy("YEAR_MONTH", "DEAL_ID", "RWA_APPROACH_CODE", 
            "REPORTING_REGULATOR_CODE","B2_IRB_CRM_FCP_ELG_FIN_COL_AMT", "B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT", 
            "B2_STD_CRM_TOT_OUTFLOW_RWA",
            "B2_STD_CRM_TOT_OUTFLOW","B2_STD_GTEE_CDS_RWA_AGG",
            "CRM_UFCP_CRED_DERIV_AMT_AGG","CRM_UFCP_GUAR_AMT_AGG",
            "CRM_FCP_ELIG_FIN_COLL_AMT_AGG","B2_STD_EFF_GTEE_AMT_AGG",
            "B2_STD_EFF_CDS_AMT_AGG","B2_STD_COLL_VOLATILITY_AMT__AGG",
            "B2_STD_COLL_MTY_MISMATCH_AMT_AGG","B2_STD_EFF_FIN_COLL_AMT_AGG",
            "B2_STD_COLL_FX_HAIRCUT_AMT_AGG","B2_IRB_COLLTRL_AMT_TTC_AGG",
            "SEC_DISCNT_B2_IRB_COLL_AMT_TTC_AGG","SEC_DISCNT_B2_IRB_GTEE_AMT_TTC_AGG",
            "BLANK_CRM_AMT_STD_AGG","BLANK_CRM_AMT_IRB_AGG",
            "RWA_POST_CRM_AMOUNT","EAD_POST_CRM_AMT", "B2_STD_OTH_FUN_CRED_PROT_AMT", "B2_IRB_CRM_UFCP_CRED_DERIV_AMT", "B2_IRB_CRM_FCP_OTH_FUNDED_AMT",
            "B2_IRB_CRM_FCP_OTH_PHY_COL_AMT", "B2_IRB_CRM_FCP_RECEIVABLES_AMT", "B2_IRB_CRM_UFCP_GUAR_AMT", "B2_IRB_GARNTEE_AMT_TTC_AGG") \
                    .agg(sum('CRM_FCP_REAL_ESTATE_AMT_INDV').alias("B2_IRB_CRM_FCP_REAL_ESTATE_AMT"));

    airb_mitigants = int_agg_mitigants_aggregated.filter("RWA_APPROACH_CODE not in ('STD')").persist(StorageLevel.MEMORY_AND_DISK_SER);

    final_int_agg_mitigants_df = int_agg_mitigants_aggregated.drop("RWA_POST_CRM_AMOUNT","EAD_POST_CRM_AMT","B2_STD_EFF_GTEE_AMT_AGG",
                                                                   "B2_IRB_NETTED_COLL_GBP", "B2_STD_EFF_CDS_AMT_AGG","B2_STD_EFF_FIN_COLL_AMT_AGG",
                                                                   "B2_STD_GTEE_CDS_RWA_AGG","b2_irb_colltrl_amt_ttc_agg", "b2_std_coll_volatility_amt__agg",
                                                                   "blank_crm_amt_irb_agg", "crm_ufcp_guar_amt_agg", "sec_discnt_b2_irb_gtee_amt_ttc_agg", 
                                                                   "blank_crm_amt_std_agg", "sec_discnt_b2_irb_coll_amt_ttc_agg", 
                                                                   "crm_fcp_elig_fin_coll_amt_agg", "b2_std_coll_mty_mismatch_amt_agg", 
                                                                   "b2_std_coll_fx_haircut_amt_agg", "crm_ufcp_cred_deriv_amt_agg", "B2_STD_OTH_FUN_CRED_PROT_AMT",
                                                                   "RWA_APPROACH_CODE", "B2_IRB_CRM_UFCP_CRED_DERIV_AMT", "B2_IRB_CRM_FCP_OTH_FUNDED_AMT",
                                                                   "B2_IRB_CRM_FCP_OTH_PHY_COL_AMT", "B2_IRB_CRM_FCP_RECEIVABLES_AMT", "B2_IRB_CRM_UFCP_GUAR_AMT",
                                                                   "B2_IRB_GARNTEE_AMT_TTC_AGG");
    
    print('\n\nTotal records in final_int_agg_mitigants layer : ', final_int_agg_mitigants_df.count());
    print("\n\n");

    final_enriched_sub_exposures = final_enriched_sub_exposures.drop("RWA_POST_CRM_AMOUNT","EAD_POST_CRM_AMT","B2_IRB_NETTED_COLL_GBP");

    final_sub_exposures = final_enriched_sub_exposures.join(final_int_agg_mitigants_df, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", "DEAL_ID"],
                                                                how="inner").distinct().persist(StorageLevel.MEMORY_AND_DISK_SER);

    print('\n\nTotal records in final_sub_exposures post int_agg enrichment : ', final_sub_exposures.count());

    #########################################################################
    ####---Broadcast Variables Creation From Enriched Sub Exposures----######
    #########################################################################

    print("\n\n **** Creating broadcast variable from final sub exposures **** \n\n");

    sub_exp_df = final_sub_exposures.select("DEAL_ID", "B2_STD_CRM_TOT_OUTFLOW", "B2_STD_EFF_FIN_COLL_AMT_AGG",
                                            "B2_STD_GTEE_CDS_RWA", "B2_STD_CRM_TOT_OUTFLOW_RWA",
                                            "B2_STD_EFF_GTEE_AMT_AGG", "B2_STD_EFF_CDS_AMT_AGG","B2_IRB_CRM_FCP_ELG_FIN_COL_AMT");

    sub_exp_pairs = sub_exp_df.rdd.map(lambda a: (a[0], (a[1], a[2], a[3], a[4], a[5], a[6],a[7]))).toDF();
    sub_exp_value_pairs = sub_exp_pairs.select(sub_exp_pairs._1.alias("Deal_Id"),
                                            sub_exp_pairs._2.alias("Enrich_SubExposure_Values")); 
    sub_exp_value_pairs_boradcast_obj = broadcast(sub_exp_value_pairs);

    ###################################################################
    #########-----------FURTHER Enriching Exposures-----------#########
    ###################################################################

    print("\n\n **** Starting exposures further enrichment **** \n\n");

    columns = ["REPORTING_REGULATOR_CODE", "EAD_POST_CRM_AMT", "YEAR_MONTH", "DEAL_ID",
            "B2_STD_EAD_POST_CRM_AMT_OFBAL", "COREP_PRODUCT_CATEGORY", "B2_STD_CCF", "DEFAULT_FUND_CONTRIB_INDICATOR",
            "B2_STD_EAD_POST_CRM_AMT_ONBAL", "CRD4_REP_STD_FLAG", "RETAIL_OFF_BAL_SHEET_FLAG",
            "ACLM_PRODUCT_TYPE", "STD_ORIG_EXP_PRE_CONV_ONBAL", "RWA_POST_CRM_AMOUNT", "CRR2_501A_DISCOUNT_FLAG",
            "v_infra_discount_factor", "B3_SME_DISCOUNT_FLAG", "SME_DISCOUNT_FACTOR", "B2_APP_RWA_POST_CRM_AMT",
            "B2_STD_RWA_POST_CRM_AMT_ONBAL", "CITIZENS_OVERRIDE_FLAG", "B2_STD_RWA_POST_CRM_AMT",
            "RET_RWA_POST_CRM_AMT_PCT", "RWA_APPROACH_CODE", "STD_RISK_WEIGHT_RATIO","B2_IRB_NETTED_COLL_GBP"
            ];

    enrich_final_exposures = final_exposures.select(columns).distinct();

    further_enrich_final_exposures = enrich_final_exposures.rdd.mapPartitions(
        further_enrich_corep_exposures(sub_exp_value_pairs_boradcast_obj)).toDF()
    further_enrich_final_exposures = further_enrich_final_exposures.withColumn("std_act_risk_weight",expr("round(std_act_risk_weight,2)"))
    further_enriched_exposures = further_enrich_final_exposures.distinct().persist(StorageLevel.MEMORY_AND_DISK_SER);
    
    final_exposures = final_exposures.distinct();
    further_enriched_final_exposures = final_exposures.join(further_enriched_exposures, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", "DEAL_ID"],
                                                            how="inner")
    further_enriched_final_exposures = further_enriched_final_exposures.distinct().persist(StorageLevel.MEMORY_AND_DISK_SER);

    print("\n\n **** Exposures further enriched **** \n\n");

    ########################################################################
    ####  ---Broadcast Variables Creation From Enriched Exposures   ---#####
    ########################################################################

    print("\n\n **** Creating broadcast variable from further enriched exposures **** \n\n");

    required_cols = ["DEAL_ID", "EAD_POST_CRM_AMT", "COREP_PRODUCT_CATEGORY",
                    "B2_STD_CCF_INPUT", "CRR2_501A_DISCOUNT_FLAG", "B2_STD_RWA_AMT_EXPONE", "SME_DISCOUNT_FACTOR",
                    "V_INFRA_DISCOUNT_FACTOR", "B3_SME_DISCOUNT_FLAG", "RWA_POST_CRM_AMOUNT", "RWA_APPROACH_CODE",
                    "STD_RISK_WEIGHT_RATIO", "RISK_TAKER_RRU", "RISK_ON_RRU", "INTERNAL_TRANSACTION_FLAG", 
                    "IG_EXCLUSION_FLAG", "RISK_TAKER_GS_CODE", "RISK_ON_GS_CODE", 
                    "DEFAULT_FUND_CONTRIB_INDICATOR", "BASEL_DATA_FLAG", "ACLM_PRODUCT_TYPE", "DIVISION"]

    enrich_exposures_df = further_enriched_final_exposures.select(required_cols).distinct();
    
    enrich_exposures_df_pair = enrich_exposures_df.rdd.map(lambda a: (a[0],(a[1], a[2], a[3], a[4], a[5], a[6],
                                                                            a[7], a[8], a[9], a[10], a[11],
                                                                            a[12], a[13], a[14], a[15], a[16], 
                                                                            a[17], a[18], a[19], a[20], a[21]
                                                        )))

    enrich_exposures_df_pair = spark.createDataFrame(enrich_exposures_df_pair, ["Deal_Id", "Enrich_Exposure_Values"])

    exposures_value_pair_boradcast_obj = broadcast(enrich_exposures_df_pair);

    ######################################################################
    ########-----------FURTHER Enriching Sub Exposures-----------#########
    ######################################################################

    print("\n\n **** Starting sub exposures further enrichment **** \n\n");

    columns = ["REPORTING_REGULATOR_CODE", "DEAL_ID", "YEAR_MONTH", "B2_STD_EFF_GTEE_AMT_AGG", "B2_STD_EFF_CDS_AMT_AGG",
                "B2_STD_GTEE_CDS_RWA", "B2_STD_GTEE_CDS_RWA_AGG","B2_STD_CRM_TOT_OUTFLOW","B2_STD_EFF_GTEE_AMT", "B2_STD_EFF_CDS_AMT",
                "MITIGANT_ID","B2_STD_GTOR_RW_RATIO"];
    final_sub_exposures_staging = final_sub_exposures.select(columns).distinct();

    further_enriched_sub_exposures = final_sub_exposures_staging.rdd.mapPartitions(
        further_enrich_sub_exposure(exposures_value_pair_boradcast_obj)).toDF()
    further_enriched_sub_exposures = further_enriched_sub_exposures.distinct().persist(StorageLevel.MEMORY_AND_DISK_SER);
    further_enriched_sub_exposures = final_sub_exposures.join(further_enriched_sub_exposures, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", "DEAL_ID","MITIGANT_ID"],
                                                            how="inner").persist(StorageLevel.MEMORY_AND_DISK_SER);
    
    print("\n\n **** Sub-exposures further enriched **** \n\n");

    ##############################################################
    #####----Imposing SUB-EXPOSURES columns on Exposures ----#####
    ##############################################################

    print("\n\n **** Imposing SUB-EXPOSURES columns on Exposures **** \n\n");

    further_enriched_final_exposures = get_sub_exp_cols_in_exp(further_enriched_final_exposures, airb_mitigants);

    ##############################################################
    #####---Filtering out "AIRB" records from SUB-EXPOSURES---####
    ##############################################################

    print("\n\n **** Filtering out 'AIRB' records from SUB-EXPOSURES **** \n\n");
    further_enriched_sub_exposures = further_enriched_sub_exposures.filter("TYPE == 'WG' and MITIGANT_TYPE in ('CD', 'GU')");
    further_enriched_sub_exposures = further_enriched_sub_exposures.filter("RWA_APPROACH_CODE = 'STD'");
    further_enriched_sub_exposures = further_enriched_sub_exposures.filter("(B2_STD_EFF_GTEE_AMT > 0.0 or B2_STD_EFF_CDS_AMT > 0.0)");

    ##############################################################
    ######----Imposing Exposure columns on Sub-Exposures----######
    ##############################################################

    print("\n\n **** Imposing EXPOSURES columns on SUB-EXPOSURES **** \n\n");

    further_enriched_sub_exposures = get_exp_cols_in_sub_exp(further_enriched_final_exposures, further_enriched_sub_exposures);

    ##############################################################
    ###########-----Union Of Exposures And SubExposures--#########
    ##############################################################

    print("\n\n **** Now doing union of exposures and sub-exposures **** \n\n");

    sub_expo_columns = set(i.lower() for i in further_enriched_sub_exposures.columns);
    expo_columns = set(i.lower() for i in further_enriched_final_exposures.columns);

    for column in sub_expo_columns:
        if (column not in expo_columns):
            further_enriched_final_exposures = enrich_exposure_row(further_enriched_final_exposures, column);
            

    for column in expo_columns:
        if (column not in sub_expo_columns):
            further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn(column, lit(None));
    
    further_enriched_sub_exposures = further_enriched_sub_exposures.withColumn("ADJUST_FLAG", lit("A"))
    union_df = further_enriched_final_exposures.unionByName(further_enriched_sub_exposures);
    union_df = union_df.persist(StorageLevel.MEMORY_AND_DISK_SER);
    
    # ###############################################################
    # #########-----------Creating Final CRD4 DF-----------#########
    # ###############################################################

    print("\n\n **** Entering into final CRD4 enrichment on combined dataset **** \n\n");

    columns_to_use = ["REPORTING_REGULATOR_CODE", "DEAL_ID", "YEAR_MONTH", "EAD_POST_CRM_AMT",
                    "B2_STD_CRM_TOT_OUTFLOW", "B2_APP_RWA_POST_CRM_AMT", "B2_STD_CRM_TOT_OUTFLOW_RWA",
                    "STD_RISK_WEIGHT_RATIO", "RWA_APPROACH_CODE", "CRR2_501A_DISCOUNT_FLAG", "B3_SME_DISCOUNT_FLAG",
                    "SME_DISCOUNT_FACTOR", "ACLM_PRODUCT_TYPE", "B2_IRB_DRAWN_RWA", "RETAIL_OFF_BAL_SHEET_FLAG",
                    "RWA_POST_CRM_AMOUNT", "B2_STD_GTEE_CDS_RWA", "COREP_PRODUCT_CATEGORY", "B2_STD_GTEE_CDS_RWA_AGG",
                    "B2_STD_RWA_POST_CRM_AMT_ONBAL", "V_INFRA_DISCOUNT_FACTOR", "APPROVED_APPROACH_CODE",
                    "B2_APP_EAD_POST_CRM_AMT","REGULATORY_PRODUCT_TYPE", "COREP_EXPOSURE_CATEGORY", 
                    "IRB_ORIG_EXP_PRE_CONV_OFBAL", "B2_IRB_ORIG_EXP_PRE_CON_FACTOR","SME_DISCOUNT_FACTOR", 
                    "B2_IRB_RWA_AMT_OFBAL", "B2_STD_ASSET_CLASS_BASEL_EXPOSURE_TYPE", "SUB_SECTOR",
                    "B2_IRB_ASSET_CLASS_REG_EXPOSURE_TYPE","FLOW_TYPE"
                    ];

    intrm_mart_df = union_df.select(columns_to_use);
        
    enriched_crd4_mart_df = intrm_mart_df.rdd.mapPartitions(enrich_crd4_df).toDF(); 
    print("\n\n **** Final enrichment completed **** \n\n");
        
    print("\n\n **** Joining Enriched CRD4 with Union DF **** \n\n");    
    final_crd4_mart_df = union_df.join(enriched_crd4_mart_df, on=["YEAR_MONTH", "REPORTING_REGULATOR_CODE", 
                                                                "DEAL_ID", "FLOW_TYPE"], how="inner");
    
    print("\n\n Total records post joining with Union DF : ", final_crd4_mart_df.count());
    print("\n\n");
    
    # Some column being dropped as they are not the column with final logic
    # This code piece to be reviewed later in recon as suggested by aviral
    final_crd4_mart_df = final_crd4_mart_df.distinct().withColumn("COREP_STD_ASSET_CLASS_REG_EXPOSURE_TYPE", lit(""))\
                        .drop('b2_std_crm_tot_outflow').drop('B2_STD_EFF_CDS_AMT')\
                        .drop('B2_STD_EFF_GTEE_AMT').drop('b2_std_eff_fin_coll_amt')\
                        .drop('B2_STD_RWA_AMT_ONBAL')\
                        .withColumnRenamed('b2_std_crm_tot_outflow_final','b2_std_crm_tot_outflow')\
                        .withColumnRenamed('B2_STD_EFF_CDS_AMT_FINAL','B2_STD_EFF_CDS_AMT')\
                        .withColumnRenamed('B2_STD_EFF_GTEE_AMT_FINAL','B2_STD_EFF_GTEE_AMT')\
                        .withColumnRenamed('b2_std_eff_fin_coll_amt_final','b2_std_eff_fin_coll_amt')\
                        .withColumnRenamed('B2_STD_RWA_AMT_ONBAL_FINAL','B2_STD_RWA_AMT_ONBAL')

#   Writing the data into s3 bucket
    print("\nWriting Final Crd4 Records Here : srtd_mart.rwa_reporting_mart table.\n");
    final_crd4_mart_df = final_crd4_mart_df.select('deal_id', 'flow_type', 'deal_id_prefix', 'valid_flag', 'unsettled_factor_code', 'unsettled_period_code', 'adjust_flag', 'reporting_type_code', 'bank_base_role_code', 'accrued_int_on_bal_amt', 'accrued_interest_gbp', 'approved_approach_code', 'pd_model_code', 'b2_irb_asset_class_reg_exposure_sub_type', 'b2_irb_asset_class_basel_exposure_type', 'b2_std_asset_class_basel_exposure_type', 'b1_rwa_post_crm_amt', 'b2_app_adjusted_lgd_ratio', 'b2_app_adjusted_pd_ratio', 'b2_app_ead_pre_crm_amt', 'b2_irb_ead_pre_crm_amt', 'b2_std_ead_pre_crm_amt', 'b2_irb_ead_post_crm_amt', 'b2_std_ead_post_crm_amt', 'b2_irb_rwa_post_crm_amt', 'b2_app_ead_post_crm_amt', 'b2_std_rwa_post_crm_amt', 'b2_app_expected_loss_amt', 'b2_irb_netted_coll_gbp', 'b2_app_risk_weight_ratio', 'b2_app_rwa_post_crm_amt', 'b2_irb_default_date', 'b2_irb_drawn_ead', 'b2_irb_drawn_expected_loss', 'b2_irb_drawn_rwa', 'b2_irb_effective_maturity_yrs', 'b2_irb_orig_exp_pre_con_factor', 'b2_irb_undrawn_ead', 'b2_irb_undrawn_expected_loss', 'b2_irb_undrawn_rwa', 'b2_std_ccf_input', 'b2_std_ccf_less_than_year', 'b2_std_ccf_more_than_year', 'b2_std_credit_quality_step', 'b2_std_default_date', 'b2_std_drawn_ead', 'b2_std_drawn_rwa', 'b2_std_exp_haircut_adj_amt', 'sys_code', 'b2_std_netted_coll_gbp', 'gross_dep_utilisation_amt', 'net_dep_utilisation_amt', 'b2_std_orig_exp_pre_con_factor', 'b2_std_risk_weight_ratio', 'b2_std_undrawn_ead', 'b2_std_undrawn_rwa', 'b3_ccp_margin_n_initl_rwa_amt', 'b3_ccp_margin_initl_cash_amt', 'b3_ccp_mar_initl_non_cash_amt', 'b3_ccp_margin_initl_rwa_amt', 'b3_cva_provision_amt', 'b3_cva_provision_residual_amt', 'jurisdiction_flag', 'capital_cuts', 'basel_data_flag', 'crr2_flag', 'coll_good_prov_amt', 'comm_less_than_1_year', 'comm_more_than_1_year', 'cva_charge', 'ifrs9_prov_impairment_amt_gbp', 'ifrs9_prov_write_off_amt_gbp', 'internal_transaction_flag', 'net_exposure', 'net_exposure_post_sec', 'off_bal_led_exp_gbp_post_sec', 'off_balance_exposure', 'on_bal_led_exp_gbp_post_sec', 'on_balance_ledger_exposure', 'past_due_asset_flag', 'provision_amount_gbp', 'residual_maturity_days', 'residual_value', 'retail_off_bal_sheet_flag', 'risk_on_group_structure', 'risk_taker_group_structure', 'securitised_flag', 'set_off_indicator', 'srt_flag', 'trading_book_flag', 'type', 'undrawn_commitment_amt', 'undrawn_commitment_amt_pst_sec', 'unsettled_amount', 'lease_exposure_gbp', 'lease_payment_value_amt', 'obligors_count', 'std_exposure_default_flag', 'crd4_sov_substitution_flag', 'unlikely_pay_flag', 'crd4_std_rp_sme_flag', 'crd4_irb_rp_sme_flag', 'b3_sme_discount_flag', 'irb_sme_discount_app_flag', 'transitional_portfolio_flag', 'fully_complete_sec_flag', 'saf_film_leasing_flag', 'principal_amt', 'mtm', 'amortisation_amt', 'bsd_marker', 'uk_sic_code', 'immovable_prop_indicator', 'corep_crm_reporting_flag', 'qual_rev_exp_flag', 'comm_real_estate_sec_flag', 'mortgages_flag', 'pers_acc_flag', 'ccp_risk_weight', 'cs_proxy_used_flag', 'b2_irb_interest_at_default_gbp', 'reil_amt_gbp', 'reil_status', 'rp_turnover_max_eur', 'rp_turnover_ccy', 'rp_sales_turnover_source', 'b3_sme_rp_flag', 'b3_sme_rp_irb_corp_flag', 'b3_sme_rp_1_5m_flag', 'rp_sales_turnover_local', 'total_asset_local', 'le_total_asset_amount_eur', 'market_risk_sub_type', 'le_fi_avc_category', 'b2_irb_rwa_r_cor_coeff', 'b2_irb_rwa_k_capital_req_pre', 'b2_irb_rwa_k_capital_req_post', 'scale_up_coefent_of_corelation', 'b3_cva_cc_adv_flag', 'llp_rru', 'rru_rk', 'b3_cva_cc_adv_var_avg', 'b3_cva_cc_adv_var_spot', 'b3_cva_cc_adv_var_stress_avg', 'b3_cva_cc_adv_var_stress_spot', 'b3_cva_cc_capital_amt', 'b3_cva_cc_rwa_amt', 'b3_cva_cc_cds_sngl_nm_ntnl_amt', 'b3_cva_cc_cds_index_ntnl_amt', 'b2_algo_irb_corp_sme_flag', 'statutory_ledger_balance', 'carrying_value', 'leverage_exposure', 'standalone_entity', 'rp_turnover_source_type', 'ifrs9_final_iis_gbp', 'tot_regu_prov_amt_gbp', 'net_mkt_val', 'b2_app_cap_req_post_crm_amt', 'grdw_pool_id', 'new_in_default_flag', 'retail_pool_sme_flag', 'grdw_pool_group_id', 'b2_irb_ccf', 'drawn_ledger_balance', 'e_star_gross_exposure', 'b2_irb_rwa_pre_cds', 'lgd_calc_mode', 'definitive_slot_category_calc', 'repu_comm_more_than_year_gbp', 'repu_comm_less_than_year_gbp', 'uncomm_undrawn_amt_gbp', 'total_undrawn_amount_gbp', 'collectively_evaluated_flag', 'original_currency_code', 'original_trade_id', 'e_star_net_exposure', 'risk_taker_rf_flag', 'risk_taker_gs_code', 'risk_on_rf_flag', 'risk_on_gs_code', 'borrower_gs_code', 'sts_securitisation', 'sts_sec_qual_cap_trtmnt', 'sts_sec_approach_code', 'b3_app_rwa_inc_cva_cc_amt', 'ifrs9_tot_regu_ecl_amt_gbp', 'ifrs9_final_ecl_mes_gbp', 'ifrs9_final_stage', 'ifrs9_stage_3_type', 'ifrs9_ecl_currency_code', 'ifrs9_tot_statut_ecl_amt_gbp', 'ifrs9_discnt_unwind_amt_gbp', 'b1_ead_post_crm_amt', 'b1_risk_weight_ratio', 'b1_rwa_pre_crm_amt', 'b1_undrawn_commitment_gbp', 'b2_irb_rwa_pre_crm_amt', 'b2_std_rwa_pre_crm_amt', 'facility_matching_level', 'product_family_code', 'carthesis

    final_crd4_mart_df.write.format("parquet").mode("overwrite").insertInto(srtd_mart_db + '.' + "rwa_reporting_mart_june", overwrite=True);
    
    end = time.time()
    print("\n\nTotal time taken for mart job completion (in seconds) : ", end-start);
    print("\n\n");
#	end of main()