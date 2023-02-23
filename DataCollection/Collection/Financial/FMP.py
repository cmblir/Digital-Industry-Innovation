import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
# Financial Statments가 있는 기업들의 Symbol을 가져오기 위한 모듈 설치 
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import certifi
from tqdm import tqdm
from datetime import datetime
import math
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


class FMP_Extracter():
    def __init__(self):
        self.core_cols = ['date','symbol','reportedCurrency','calendarYear','period','link','finalLink'] 
        self.is_cols = ['revenue','costOfRevenue','sellingGeneralAndAdministrativeExpenses','operatingIncome','totalOtherIncomeExpensesNet',
                'incomeBeforeTax','incomeTaxExpense','netIncome','operatingIncomeRatio'] 
        self.bs_cols = ['totalCurrentAssets','totalNonCurrentAssets','totalAssets', 'totalCurrentLiabilities','totalNonCurrentLiabilities',
                'totalLiabilities','totalEquity','totalLiabilitiesAndTotalEquity']
        self.cf_cols = ['netCashProvidedByOperatingActivities','netCashUsedForInvestingActivites','netCashUsedProvidedByFinancingActivities',
                'cashAtBeginningOfPeriod','cashAtEndOfPeriod']
        self.FMP_field = {
                '채워야할 테이블 필드명':
                ['keyval','stock_mrkt_cd','acplc_lngg_stock_mrkt_nm','engls_stock_mrkt_nm','hb_ntn_cd','lstng_cd','acplc_lngg_entrp_nm','engls_entrp_nm','ovrss_entrp_crprt_rgno','ovrss_entrp_bsnsm_rgno','fndtn_dt','lstng_dt',
                'acplc_lngg_indst_gnnm','engls_indstrsctrs_nm','crrnc_sctin_cd','accnn_yr','reprt_kind_cd','stacnt_dt','cuass_amt','nncrrnt_assts_amt','cash_and_dpst_amt','scrts_amt','lon_bond_amt','instm_fnc_assts_amt','lease_assts_amt',
                'tpe_assts_amt','etc_assts_amt','assts_summ','fltng_debt_amt','nncrrnt_debt_amt','cstdpslblits_amt','cstdbt_amt','etc_debt_amt','debt_summ','captl','captl_srpl','captl_mdtn_amt','etc_inclsn_prlss_acttl_amt','prft_srpl',
                'captl_summ','debt_captl_summ_amt','prsls','sllng_prmpc_amt','bsn_cost_amt','bsn_prft_amt','bsn_else_cost_amt','ctax_cost_strbf_ntincmls_amt','ctax_cost_strbf_cntntbs_plamt','ctax_cost_amt','cntntbs_prlss_ctax_cost_amt',
                'cntntbs_prft_amt','dscnt_bsnss_prlss_amt','thtrm_ntpf_amt','bsn_acti_csflw_amt','invsm_acti_cash_infl_amt','fnnr_acti_cash_infl_amt','cash_incre_amt','bsis_cash_amt','entrm_cash_amt','debt_rate','bsn_prft_rt_val','prsls_incre_rt',
                'bsn_prft_incre_rt_val','thtrm_ntpf_incre_rt_val','entrp_yrmn_grrt_val','entrp_reltn_tdngs_dt','entrp_reltn_tdngs_kind_cont','entrp_reltn_tdngs_subjc','entrp_reltn_tdngs_cont_smmr','entrp_reltn_tdngs_dtl_cont','info_orgin_cont',
                'entrp_reltn_tdngs_url','cmp_insd_reltn_info','cstmr_reltn_info','sppl_reltn_info','cmptt_reltn_info','sbst_goods_reltn_info','opert_sctin_cd','data_crtin_dt','cntct_prces_stts_cd','cntct_prces_dt'],
                "Financial Modeling API":
                ['exchangeShortName','exchange','exchange','헤브론스타국가코드','symbol','companyName','companyName','','taxIdentificationNumber','','ipoDate','industry','industry','reportedCurrency','calendarYear',
                'period','date','totalCurrentAssets','totalNonCurrentAssets','cashAndCashEquivalents','','','','','propertyPlantEquipmentNet','otherAssets','totalAssets','totalCurrentLiabilities','totalNonCurrentLiabilities','',
                '','otherLiabilities','totalLiabilities','','','','accumulatedOtherComprehensiveIncomeLoss','retainedEarnings','totalEquity','totalLiabilitiesAndTotalEquity','revenue','costOfRevenue','operatingExpenses','operatingIncome','',
                'incomeBeforeTax','','incomeTaxExpense','','','','netIncome_x','netCashProvidedByOperatingActivities','netCashUsedForInvestingActivites','netCashUsedProvidedByFinancingActivities','netChangeInCash','cashAtBeginningOfPeriod','cashAtEndOfPeriod','','','','','','','','','','','','','','','','','','','','','','','']}


    def get_jsonparsed_data(self, url):
        """
        Receive the content of ``url``, parse it as JSON and return the object.

        Parameters
        ----------
        url : str

        Returns
        -------
        dict
        """
        response = urlopen(url, cafile=certifi.where())
        data = response.read().decode("utf-8")
        return json.loads(data)

    def extractor(url) : 
        try:
            from pandas import json_normalize
        except ImportError:
            from pandas.io.json import json_normalize
        # 데이터 갖고오기
        req=requests.get(url)
        # json load
        data = json.loads(req.text)
        # json to pandas
        preprocessed = json_normalize(data)
        return preprocessed

    def url_generator(self, target_Symbol, filing_type, limit, period='quarter') : 
        base_url = 'https://financialmodelingprep.com/api/v3/income-statement/RY.TO?limit=5&period=quarter&apikey=89d4891348727c3950b79b9067127c3f'
        if period == 'annual' : 
            base_url = base_url.replace('&period=quarter','')
        
        is_url = base_url.replace('RY.TO', target_Symbol)
        is_url = is_url.replace('=5', '='+str(limit))
        bs_url = is_url.replace('income-statement','balance-sheet-statement')
        cf_url = is_url.replace('income-statement','cash-flow-statement')
        
        if filing_type == 'is' : 
            return is_url
        elif filing_type == 'bs' : 
            return bs_url
        elif filing_type == 'cf' : 
            return cf_url 
        else :
            return "Please input proper filing_type"

    def ending_period_extract(x) : 
        try : 
            date = datetime.strptime(str(x), '%Y-%m-%d')
            return date.strftime("%Y%m%d")
        except : 
            return 

    def report_type_extract(x) : 
        try : 
            if 'Q' in str(x) : 
                return 'Q'
            elif 'FY' in str(x) : 
                return 'A'
            else : 
                return 
        except : 
            return 

    def cleanse(self, path, filename, fund = False, trading = True) : 
        original_df = pd.read_excel(path)
        """
        데이터 가공하는 함수
        original_df : extract_and_save에서 추출한 원본데이터 \n 
        save_dir : 저장할 디렉토리 \n
        fund : 주식시장에 상장된 etf 까지 확인하고 싶으면 True, 아니라면 False \n 
        trading : 현재 주식시장에서 거래되고 있는 기업들만 추출하고 싶으면 True, 아니라면 False
        """

        if fund == True : 
            pass  # 펀드포함 
        else : 
            # 펀드가 제거된, 진짜 상장기업 정보만을 보고싶으면 false 
            original_df['isFund'].fillna(False) 
            original_df = original_df[original_df['isFund']== False]
            
            
        # 현재 거래되고 있는 기업들만 추출 
        if trading == True : 
            # isActivelyTrading 이 공란인 기업들의 경우 거래되고 있다고 가정
            original_df['isActivelyTrading'] = original_df['isActivelyTrading'].fillna(True)
            original_df = original_df[original_df['isActivelyTrading']==True]
        else : 
            pass
        
        
        ## 대응표 갖고오기 
        mapping_sheet = pd.DataFrame(self.FMP_field)
        # 대응표를 딕셔너리화 key : value = 기준필드명 : FMP필드명
        mapping_dic = mapping_sheet.set_index('채워야할 테이블 필드명').T.to_dict('index')['Financial Modeling API']
        
        
        # processed라는 빈 데이터 프레임을 생성한다. 
        processed = pd.DataFrame(columns = mapping_dic.keys())
        
        # 아래 루프로 대응표를 이용하여 processed라는 빈 데이터 프레임에 정보를 집어 넣는다. 
        for order, field in enumerate(processed.columns) : 
            try : 
                processed.loc[:,field] = original_df.loc[:,mapping_dic[field]]
            except : 
                pass
        import re 
        regExp = '\W[a-zA-Z]+'  # \W로 문자, 숫자가 아닌 특수문자 제거하고 [a-zA-Z]+ 로 ''. 뒤에있는 주식시장 코드 제거
        processed['lstng_cd']= processed['lstng_cd'].str.replace(pat = regExp ,repl=r'', regex = True)
        processed['stacnt_dt'] = processed['stacnt_dt'].apply(lambda x : self.ending_period_extract(x))
        processed['lstng_dt'] = processed['lstng_dt'].apply(lambda x : self.ending_period_extract(x))
        processed['fndtn_dt'] = processed['fndtn_dt'].apply(lambda x : self.ending_period_extract(x))
        processed['reprt_kind_cd'] = processed['reprt_kind_cd'].apply(lambda x : self.report_type_extract(x))
        
        # 엑셀로 저장
        processed.to_excel(f"finished_{filename}.xlsx", index = False)
        print(f"finished_{filename}이 완료되었습니다.")
        
        # 판다스로 리턴
        return processed

    def get_symbols(self, country):
        """
        데이터 가져오는 함수 \n
        county에 국가명을 입력하세요.
        """
        # Financial Statments가 있는 기업들의 Symbol을 가져오기 위한 QueryString. API Key = 33d67d9c5e5ed94ff134e9bf93f9d818
        url = ("https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey=33d67d9c5e5ed94ff134e9bf93f9d818")
        Symbols = self.get_jsonparsed_data(url)
        # Financial Statments가 있는 기업들의 Symbol을 가져오기 위한 QueryString. API Key = 33d67d9c5e5ed94ff134e9bf93f9d818
        url = ("https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey=33d67d9c5e5ed94ff134e9bf93f9d818")

        Symbols = self.get_jsonparsed_data(url)

        US_symbols = []          # 
        Canada_symbols = []      #TO
        France_symbols = []      #PA
        Germany_symbols = []     #DE
        India_symbols = []       #NS
        London_symbols = []      #L
        Hongkong_symbols = []    #HK
        Australia_symbols = []  #AX
        Swiss_symbols = []       #SW
        Korea_symbols = []       #KR
        Netherlands_symbols = []

        for Symbol in Symbols : 
            if ".TO" in Symbol : 
                Canada_symbols.append(Symbol) 
            elif ".PA" in Symbol : 
                France_symbols.append(Symbol) 
            elif ".DE" in Symbol : 
                Germany_symbols.append(Symbol) 
            elif ".NS" in Symbol : 
                India_symbols.append(Symbol) 
            elif ".BS" in Symbol : 
                India_symbols.append(Symbol) 
            elif ".L" in Symbol : 
                London_symbols.append(Symbol) 
            elif ".HK" in Symbol : 
                Hongkong_symbols.append(Symbol) 
            elif ".AX" in Symbol : 
                Australia_symbols.append(Symbol) 
            elif ".SW" in Symbol : 
                Swiss_symbols.append(Symbol) 
            elif ".KS" in Symbol :
                Korea_symbols.append(Symbol)
            elif "." not in Symbol :
                US_symbols.append(Symbol) 
            elif ".EU" in Symbol : 
                Netherlands_symbols.append(Symbol)


        cnt = 0
        if country == "캐나다" : selected_symbols = Canada_symbols
        elif country == "프랑스" : selected_symbols = France_symbols
        elif country == "독일" : selected_symbols = Germany_symbols
        elif country == "인도" : selected_symbols = India_symbols
        elif country == "영국" : selected_symbols = London_symbols
        elif country == "홍콩" : selected_symbols = Hongkong_symbols
        elif country == "호주" : selected_symbols = Australia_symbols
        elif country == "스위스" : selected_symbols = Swiss_symbols
        elif country == "한국" : selected_symbols = Korea_symbols
        elif country == "미국" : selected_symbols = US_symbols
        elif country == "네덜란드" : selected_symbols = Netherlands_symbols

        for _ in range(math.ceil(len(selected_symbols)/1000)):
            company_df_list = pd.DataFrame()
            for target_Symbol in tqdm(India_symbols[cnt:cnt+1000]) :  
                if cnt == len(India_symbols):
                    company_df_list = company_df_list.sort_values(by=['symbol','date'], ascending = [True,False]) # symbol은 오름차순으로, 같은 심볼 내에서 회계분기는 내림차순이 되도록 정렬.        
                    # 2022년 현재, 5개년치의 데이터, 즉 2017년 이상인 데이터만 뽑아낸다. -> calendarYear(회계연도)가 2017년 이상 
                    # company_df_list = company_df_list[company_df_list['calendarYear'].astype('int')>=2022]
                    company_df_list.to_excel(f'US_Original_221216_{cnt}.xlsx', index=False)

                for report_type in ['annual','quarter'] :
                
                    if report_type == 'annual' : 
                        limit = 2 # 연간 재무제표 1개년 
                    else : 
                        limit = 8 # 분기 별 재무제표 (1년에 4분기)
                
            
                    # 기업별 URL 생성 
                    is_url = self.url_generator(target_Symbol, 'is', limit, report_type)
                    bs_url = self.url_generator(target_Symbol, 'bs', limit, report_type)
                    cf_url = self.url_generator(target_Symbol, 'cf', limit, report_type)

                    # extractor 함수를 이용하여 API를 통한 데이터를 판다스 데이터프레임으로 갖고옴. 
                    df_is = self.extractor(is_url)
                    df_bs = self.extractor(bs_url)
                    df_cf = self.extractor(cf_url)


                # 오류 회피 
                # 간혹가다 손익 계산서, 대차대조표는 있는데 현금흐름표가 없는등의 문제가 있다. 
                # 모든 경우에 대한 오류 회피 
                # try : 만약 추출해 올 열들에 대한 정보가 존재한다면 불러온다
                # except : 만약 추출 해 올 열들에 대한 정보가 있으면, core_is()

                    try :  
                        df_is = df_is[self.core_cols+self.is_cols]
                    except : 
                        df_is = pd.DataFrame(columns=self.core_cols+self.is_cols)    
                    try :     
                        df_bs= df_bs[self.core_cols+self.bs_cols]
                    except : 
                        df_bs = pd.DataFrame(columns=self.core_cols+self.bs_cols)

                    try : 
                        df_cf = df_cf[self.core_cols+self.cf_cols]
                    except : 
                        df_cf = pd.DataFrame(columns=self.core_cols+self.cf_cols)


                    # merge above three dataframes 
                    #company_df = pd.concat([df_is,df_bs,df_cf],axis = 1)
                    company_df = pd.merge(df_is, df_bs, how = 'outer', on = self.core_cols)
                    company_df = pd.merge(company_df, df_cf, how = 'outer', on = self.core_cols)

                    # 숫자 데이터 이외에 필요한 기업 일반정보들을 추가한다. 
                    # Financil Modeling Prep의 Company profile API를 이용한다. 
                    base_url = 'https://financialmodelingprep.com/api/v3/profile/RY.TO?apikey=89d4891348727c3950b79b9067127c3f'
                    target_url = base_url.replace('RY.TO', target_Symbol)

                    # request로 데이터 갖고오기 
                    req = requests.get(target_url)
                    data = json.loads(req.text)

                    # 데이터 명세서에 근거하여 필요한 일반정보 칼럼들만 추출하여 company_df에 추가한다. 

                    try : company_df['companyName'] = data[0]['companyName'] 
                    except :company_df['companyName'] = ""
                    try : company_df['ceo'] = data[0]['ceo']
                    except :company_df['ceo'] = ""
                    try : company_df['phone'] =data[0]['phone']
                    except : company_df['phone'] = ""
                    try : company_df['website'] =data[0]['website']
                    except : company_df['website'] = ""
                    try : company_df['state'] =data[0]['state']
                    except : company_df['state'] = ""
                    try : company_df['city'] =data[0]['city']
                    except : company_df['city'] = ""
                    try : company_df['country'] =data[0]['country']
                    except : company_df['country'] = ""
                    try : company_df['industry'] =data[0]['industry']
                    except : company_df['industry'] = ""
                    try : company_df['ipoDate'] =data[0]['ipoDate']
                    except : company_df['ipoDate'] = ""
                    try : company_df['address'] =data[0]['address']
                    except : company_df['address'] = ""
                    try : company_df['zip'] =data[0]['zip']
                    except : company_df['zip'] = ""
                    try : company_df['exchangeShortName'] =data[0]['exchangeShortName']
                    except : company_df['exchangeShortName'] = ""
                    try : company_df['exchange'] =data[0]['exchange']
                    except :  company_df['exchange'] = ""
                    try : company_df['description'] =data[0]['description']
                    except : company_df['description'] = ""
                    try : company_df['isEtf'] =data[0]['isEtf']
                    except : company_df['isEtf'] = ""
                    try : company_df['isActivelyTrading'] =data[0]['isActivelyTrading']
                    except : company_df['isActivelyTrading'] = ""
                    try : company_df['isFund'] =data[0]['isFund']
                    except : company_df['isFund'] = ""
                        
                    # 택스 번호 추가. 'Company core information API'를 이용한다. 
                    try : 
                        base_url = 'https://financialmodelingprep.com/api/v4/company-core-information?symbol=AAPL&apikey=89d4891348727c3950b79b9067127c3f'
                        target_url = base_url.replace('AAPL', target_Symbol)

                        # request로 데이터 갖고오기 
                        # 갖고오는 데이터가 null이게 되면, 'data'에서 정보를 추출하는 과정에서 에러가 발생하게 될 것이다. 
                        req = requests.get(target_url)
                        data = json.loads(req.text)
                        
                        company_df['taxIdentificationNumber'] =data[0]['taxIdentificationNumber']
                        company_df['registrantName'] =data[0]['registrantName']

                    except : 
                        company_df['taxIdentificationNumber'] = ""
                        company_df['registrantName'] = ""
                    
                        
                        
                    company_df_list = company_df_list.append(company_df)
                    cnt += 1
                
                
            company_df_list = company_df_list.sort_values(by=['symbol','date'], ascending = [True,False]) # symbol은 오름차순으로, 같은 심볼 내에서 회계분기는 내림차순이 되도록 정렬.        

            # 2022년 현재, 5개년치의 데이터, 즉 2017년 이상인 데이터만 뽑아낸다. -> calendarYear(회계연도)가 2017년 이상 
            # company_df_list = company_df_list[company_df_list['calendarYear'].astype('int')>=2022]
            company_df_list.to_excel(f'Original_{cnt}.xlsx', index=False)

    def make_clean(self, FilePath, SavePath):
        """
        FilePath = Clean 작업을 진행할 디렉토리 \n
        SavePath = Clean 작업 후 저장할 디렉토리
        """
        for File in tqdm(FilePath):
            name = File.split(".")[0]
            self.cleanse(f"{SavePath}/{File}", name)