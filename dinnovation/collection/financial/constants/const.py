korea_financial_column = ["키값","주식시장코드","현지언어주식시장명",
"영문주식시장명","헤브론스타국가코드","상장코드","현지언어기업명","영문기업명",
"법인등록번호","사업자등록번호","설립일자","상장일자","현지언어산업군명",
"영문산업군명","통화구분코드","회계연도","보고서종류코드","결산일자",
"유동자산금액","비유동자산금액","현금및예치금액","유가증권금액","대출채권금액",
"할부금융자산금액","리스자산금액","유형자산금액","기타자산금액","자산총계",
"유동부채금액","비유동부채금액","예수부채금액","차입부채금액","기타부채금액",
"부채총계","자본금","자본잉여금","자본조정금액","기타포괄손익누계액","이익잉여금",
"자본총계","부채자본총계액","매출액","매출원가금액","영업비용금액","영업이익금액",
"영업외비용금액","법인세비용차감전순손익금액","법인세비용차감전계속사업손익금액","법인세비용금액",
"계속사업손익법인세비용금액","계속사업이익금액","중단사업손익금액","당기순이익금액",
"영업활동현금흐름금액","투자활동현금유입금액","재무활동현금유입금액","현금증가금액",
"기초현금금액","기말현금금액","부채비율","영업이익율값","매출액증가율","영업이익증가율값",
"당기순이익증가율값","기업연평균성장률값","기업관련소식날짜","기업관련소식종류내용","기업관련소식제목",
"기업관련소식내용요약","기업관련소식상세내용","정보출처내용","기업관련소식URL","사내관련정보",
"고객관련정보","공급관련정보","경쟁관련정보","대체재관련정보","작업구분코드","데이터생성일자",
"연계처리상태코드","연계처리일자"]

fmp_core_cols = ['date','symbol','reportedCurrency','calendarYear','period','link','finalLink'] 
fmp_is_cols = ['revenue','costOfRevenue','sellingGeneralAndAdministrativeExpenses','operatingIncome','totalOtherIncomeExpensesNet',
        'incomeBeforeTax','incomeTaxExpense','netIncome','operatingIncomeRatio'] 
fmp_bs_cols = ['totalCurrentAssets','totalNonCurrentAssets','totalAssets', 'totalCurrentLiabilities','totalNonCurrentLiabilities',
        'totalLiabilities','totalEquity','totalLiabilitiesAndTotalEquity']
fmp_cf_cols = ['netCashProvidedByOperatingActivities','netCashUsedForInvestingActivites','netCashUsedProvidedByFinancingActivities',
        'cashAtBeginningOfPeriod','cashAtEndOfPeriod']
fmp_FMP_field = {
                '채워야할 테이블 필드명':
                ['keyval','stock_mrkt_cd','acplc_lngg_stock_mrkt_nm','engls_stock_mrkt_nm','hb_ntn_cd','lstng_cd','acplc_lngg_entrp_nm','engls_entrp_nm','ovrss_entrp_crprt_rgno','ovrss_entrp_bsnsm_rgno','fndtn_dt','lstng_dt',
                'acplc_lngg_indst_gnnm','engls_indstrsctrs_nm','crrnc_sctin_cd','accnn_yr','reprt_kind_cd','stacnt_dt','cuass_amt','nncrrnt_assts_amt','cash_and_dpst_amt','scrts_amt','lon_bond_amt','instm_fnc_assts_amt','lease_assts_amt',
                'tpe_assts_amt','etc_assts_amt','assts_summ','fltng_debt_amt','nncrrnt_debt_amt','cstdpslblits_amt','cstdbt_amt','etc_debt_amt','debt_summ','captl','captl_srpl','captl_mdtn_amt','etc_inclsn_prlss_acttl_amt','prft_srpl',
                'captl_summ','debt_captl_summ_amt','prsls','sllng_prmpc_amt','bsn_cost_amt','bsn_prft_amt','bsn_else_cost_amt','ctax_cost_strbf_ntincmls_amt','ctax_cost_strbf_cntntbs_plamt','ctax_cost_amt','cntntbs_prlss_ctax_cost_amt',
                'cntntbs_prft_amt','dscnt_bsnss_prlss_amt','thtrm_ntpf_amt','bsn_acti_csflw_amt','invsm_acti_cash_infl_amt','fnnr_acti_cash_infl_amt','cash_incre_amt','bsis_cash_amt','entrm_cash_amt','debt_rate','bsn_prft_rt_val','prsls_incre_rt',
                'bsn_prft_incre_rt_val','thtrm_ntpf_incre_rt_val','entrp_yrmn_grrt_val','entrp_reltn_tdngs_dt','entrp_reltn_tdngs_kind_cont','entrp_reltn_tdngs_subjc','entrp_reltn_tdngs_cont_smmr','entrp_reltn_tdngs_dtl_cont','info_orgin_cont',
                'entrp_reltn_tdngs_url','cmp_insd_reltn_info','cstmr_reltn_info','sppl_reltn_info','cmptt_reltn_info','sbst_goods_reltn_info','opert_sctin_cd','data_crtin_dt','cntct_prces_stts_cd','cntct_prces_dt'],
                "Financial Modeling API":
                ["","exchangeShortName","exchange","exchange","헤브론스타국가코드","symbol","companyName","companyName","","taxIdentificationNumber",
                "","ipoDate","industry","industry","reportedCurrency","calendarYear","period","date","totalCurrentAssets","totalNonCurrentAssets","cashAndCashEquivalents","","","","","propertyPlantEquipmentNet","otherAssets","totalAssets","totalCurrentLiabilities",
                "totalNonCurrentLiabilities","","","otherLiabilities","totalLiabilities","","","","accumulatedOtherComprehensiveIncomeLoss","retainedEarnings","totalEquity","totalLiabilitiesAndTotalEquity","revenue","costOfRevenue","operatingExpenses","operatingIncome",
                "","incomeBeforeTax","","incomeTaxExpense","","","","netIncome_x","netCashProvidedByOperatingActivities","netCashUsedForInvestingActivites","netCashUsedProvidedByFinancingActivities","netChangeInCash","cashAtBeginningOfPeriod","cashAtEndOfPeriod",
                "","","","","","","","","","","","","","","","","","","","","",""]}