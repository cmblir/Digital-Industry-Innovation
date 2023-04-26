import re
import pandas as pd
from tqdm import tqdm
import random
import numpy as np
import warnings
import datetime
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('mode.chained_assignment',  None)

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리는 ETL과정중 T(Transform)을 포함하고 있습니다. \n
        데이터베이스에서 데이터를 확인하는 클래스는 Checker 입니다. \n
        클래스 지정시 데이터베이스(postgresql)의 id, pw, ip, db, 추출하고자 하는 테이블명을 입력해주세요. \n
        read_excel() 함수는 xlsx을 불러와 데이터프레임으로 저장합니다. \n
        read_csv() 함수는 csv를 불러와 데이터프레임으로 저장합니다. \n
        data_update() 함수는 신규 데이터의 업데이트시 I 또는 U를 입력합니다. \n
        date_update() 함수는 신규 데이터의 업데이트시 날짜를 입력합니다. \n
        CheckDate() 함수는 인베스팅 닷컴의 일반 데이터 날짜를 표준화하는 함수 \n
        CheckLength() 함수는 데이터의 크기를 확인하여 크기만큼 자르는 함수 \n
        CheckVarchar() 함수는 재무 데이터 크기를 확인하여 클 경우 새로 삽입하는 함수 \n
        CheckNumeric() 함수는 재무 데이터 숫자를 확인하는 함수 \n
        ------------------------------------------------------------------ \n
        데이터베이스에서 데이터를 확인하는 클래스는 Analysis 입니다. \n
        read_excel() 함수는 xlsx을 불러와 데이터프레임으로 저장합니다. \n
        read_csv() 함수는 csv를 불러와 데이터프레임으로 저장합니다. \n
        Fail() 함수는 오류가 난 데이터를 데이터프레임에 넣기 위해 딕셔너리화 시키는 함수 \n
        CheckDate_Duplicate() 함수는 날짜체크, 중복체크를 확인하는 함수 \n
        CheckNumber() 함수는 전화번호가 유효한지 확인하는 함수 \n
        """)

class Checker:
    """
    데이터를 검사하여 필요에 따라 값을 대체하는 모듈
    """
    def __init__(self):
        self.definition = {
        'STOCK_MRKT_CD' : 6,
        'ACPLC_LNGG_STOCK_MRKT_NM' : 60,
        'ENGLS_STOCK_MRKT_NM' : 60,
        'HB_NTN_CD' : 3,
        'LSTNG_CD' : 12,
        'ACPLC_LNGG_ENTRP_NM' : 100,
        'ENGLS_ENTRP_NM' : 100,
        'OVRSS_ENTRP_CRPRT_RGNO' : 50,
        'OVRSS_ENTRP_BSNSM_RGNO' : 50,
        'FNDTN_DT' : 8,
        'LSTNG_DT' : 8,
        'ACPLC_LNGG_INDST_GNNM' : 100,
        'ENGLS_INDSTRSCTRS_NM' : 100,
        'CRRNC_SCTIN_CD' : 3,
        'ACCNN_YR' : 4,
        'REPRT_KIND_CD' : 1,
        'STACNT_DT' : 8,
        'BSN_PRFT_RT_VAL' : 4,
        'BSN_PRFT_INCRE_RT_VAL' : 4,
        'THTRM_NTPF_INCRE_RT_VAL' : 4,
        'ENTRP_YRMN_GRRT_VAL' : 4,
        'ENTRP_RELTN_TDNGS_DT' : 8,
        'ENTRP_RELTN_TDNGS_KIND_CONT' : 10,
        'ENTRP_RELTN_TDNGS_SUBJC' : 100,
        'ENTRP_RELTN_TDNGS_CONT_SMMR' : 1000,
        'ENTRP_RELTN_TDNGS_DTL_CONT' : 4000,
        'INFO_ORGIN_CONT' : 4000,
        'ENTRP_RELTN_TDNGS_URL' : 1000,
        'CMP_INSD_RELTN_INFO' : 4000,
        'CSTMR_RELTN_INFO' : 4000,
        'SPPL_RELTN_INFO' : 4000,
        'CMPTT_RELTN_INFO' : 4000,
        'SBST_GOODS_RELTN_INFO' : 4000,
        'OPERT_SCTIN_CD' : 1,
        'DATA_CRTIN_DT' : 8,
        'CNTCT_PRCES_STTS_CD' : 1,
        'CNTCT_PRCES_DT': 8}

        self.ColumnsDict = {
        "날짜" : "ENTRP_RELTN_TDNGS_DT",
        "종류" : "ENTRP_RELTN_TDNGS_KIND_CONT",
        "제목" : "ENTRP_RELTN_TDNGS_SUBJC",
        "요약" : "ENTRP_RELTN_TDNGS_CONT_SMMR",
        "상세" : "ENTRP_RELTN_TDNGS_DTL_CONT",
        "출처" : "INFO_ORGIN_CONT",
        "URL" : "ENTRP_RELTN_TDNGS_URL",
        "사내" : "CMP_INSD_RELTN_INFO",
        "고객" : "CSTMR_RELTN_INFO",
        "공급" : "SPPL_RELTN_INFO",
        "경쟁" : "CMPTT_RELTN_INFO",
        "대체" : "SBST_GOODS_RELTN_INFO"}
        
        self.Financial = ["CUASS_AMT", "NNCRRNT_ASSTS_AMT", "CASH_AND_DPST_AMT", "SCRTS_AMT",
                    "LON_BOND_AMT", "INSTM_FNC_ASSTS_AMT", "LEASE_ASSTS_AMT", "TPE_ASSTS_AMT",
                    "ETC_ASSTS_AMT", "ASSTS_SUMM", "FLTNG_DEBT_AMT", "NNCRRNT_DEBT_AMT",
                    "CSTDPSLBLITS_AMT", "CSTDBT_AMT", "ETC_DEBT_AMT", "DEBT_SUMM", "CAPTL",
                    "CAPTL_SRPL","CAPTL_MDTN_AMT", "ETC_INCLSN_PRLSS_ACTTL_AMT", "PRFT_SRPL", "CAPTL_SUMM",
                    "DEBT_CAPTL_SUMM_AMT", "PRSLS", "SLLNG_PRMPC_AMT", "BSN_COST_AMT", "BSN_PRFT_AMT",
                    "BSN_ELSE_COST_AMT", "CTAX_COST_STRBF_NTINCMLS_AMT", "CTAX_COST_STRBF_CNTNTBS_PLAMT",
                    "CTAX_COST_AMT", "CNTNTBS_PRLSS_CTAX_COST_AMT", "CNTNTBS_PRFT_AMT",
                    "DSCNT_BSNSS_PRLSS_AMT", "THTRM_NTPF_AMT", "BSN_ACTI_CSFLW_AMT",
                    "INVSM_ACTI_CASH_INFL_AMT", "FNNR_ACTI_CASH_INFL_AMT", "CASH_INCRE_AMT",
                    "BSIS_CASH_AMT","ENTRM_CASH_AMT", "DEBT_RATE", "PRSLS_INCRE_RT"]

        self.Month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        self.NumericList = ["STACNT_DT","CUASS_AMT", "NNCRRNT_ASSTS_AMT", "CASH_AND_DPST_AMT", "SCRTS_AMT", "LON_BOND_AMT", "INSTM_FNC_ASSTS_AMT", "LEASE_ASSTS_AMT", "TPE_ASSTS_AMT", "ETC_ASSTS_AMT", "ASSTS_SUMM", "FLTNG_DEBT_AMT", "NNCRRNT_DEBT_AMT", "CSTDPSLBLITS_AMT", "CSTDBT_AMT", "ETC_DEBT_AMT", "DEBT_SUMM", "CAPTL",
        "CAPTL_SRPL", "CAPTL_MDTN_AMT", "ETC_INCLSN_PRLSS_ACTTL_AMT", "PRFT_SRPL", "CAPTL_SUMM", "DEBT_CAPTL_SUMM_AMT", "PRSLS", "SLLNG_PRMPC_AMT", "BSN_COST_AMT", "BSN_PRFT_AMT", "BSN_ELSE_COST_AMT", "CTAX_COST_STRBF_NTINCMLS_AMT", "CTAX_COST_STRBF_CNTNTBS_PLAMT", "CTAX_COST_AMT", "CNTNTBS_PRLSS_CTAX_COST_AMT",
        "CNTNTBS_PRFT_AMT", "DSCNT_BSNSS_PRLSS_AMT", "THTRM_NTPF_AMT", "BSN_ACTI_CSFLW_AMT", "INVSM_ACTI_CASH_INFL_AMT", "FNNR_ACTI_CASH_INFL_AMT", "CASH_INCRE_AMT", "BSIS_CASH_AMT", "ENTRM_CASH_AMT", "DEBT_RATE", "PRSLS_INCRE_RT"]

        self.MonthDict = {
            "Jan" : "01",
            "Feb" : "02",
            "Mar" : "03",
            "Apr" : "04",
            "May" : "05",
            "Jun" : "06",
            "Jul" : "07",
            "Aug" : "08",
            "Sep" : "09",
            "Oct" : "10",
            "Dec" : "11",
            "Nov" : "12"
        }
    
        self.df = None

    def fndtn_dt(self):
        """
        데이터 내의 fndtn_dt 값을 YYYYMMDD로 표준화해주는 함수
        """
        for idx in range(len(self.df)):
            value = self.df["fndtn_dt"][idx]
            if str(type(value)) == "<class 'numpy.int64'>": pass
            elif str(type(value)) == "<class 'datetime.datetime'>": self.df["fndtn_dt"][idx] = value.strftime("%Y%m%d")
            elif str(type(value)) == "<class 'numpy.float64'>":
                if value.astype(int) > 21000000 or value.astype(int) < 18000000:
                    if str(value) == "nan": self.df["fndtn_dt"][idx] = ""
            else:
                self.df["fndtn_dt"][idx] = ""

    def read_excel(self, path):
        """
        path에 excel 주소를 입력하세요.
        """
        self.df = pd.read_excel(path)

    def read_csv(self, path):
        """
        path에 csv 주소를 입력하세요.
        """
        self.df = pd.read_csv(path)

    def data_update(self, Insert: bool) -> bool:
        """
        신규 데이터 업데이트 여부를 입력해주세요.
        """
        if Insert == True:
            self.df["opert_sctin_cd"] = "I"
        elif Insert == False:
            self.df["opert_sctin_cd"] = "U"

    def date_update(self):
        """
        신규 데이터의 날짜 업데이트를 입력하는 함수
        """
        self.df["data_crtin_dt"] = "".join(str(datetime.date.today()).split("-"))

    def CheckDate(self):
        """
        인베스팅 닷컴의 데이터 날짜를 표준화시키는 함수 \n
        일반만 사용 가능
        """
        for TimeLength in range(len(self.df)):
            time = self.df["entrp_reltn_tdngs_dt"][TimeLength]
            test = time.split()
            if len(time.split()) != 5:
                TimeStamp = test[5] + self.MonthDict[test[3].replace("(", "")] + test[4]
            else:
                TimeStamp = test[2] + self.MonthDict[test[0]] + test[1]
            self.df["entrp_reltn_tdngs_dt"][TimeLength] = re.sub(r'[^0-9]','',TimeStamp)
    
    def CheckLength(self):
        '''
        데이터의 크기보다 클 경우 해당 데이터 크기까지 자르는 함수 \n
        재무, 일반 모두 가능
        '''
        for key, value in self.definition.items():
            for Length in range(len(self.df["keyval"])):
                check = str(self.df[key.lower()][Length])
                
                if len(check) > value:
                    self.df[key.lower()][Length] = str(self.df[key.lower()][Length])[:value]
                else:
                    pass

    def CheckVarchar(self, News=False): 
        """
        데이터의 크기를 체크하고 만약 데이터 크기보다 클 경우 해당 데이터를 새로 삽입하는 함수 \n
        재무만 가능
        """
        FirstRange = len(self.df['keyval'])
        cnt = 0
        
        for Length in range(FirstRange, FirstRange + cnt):
            self.df[self.ColumnsDict["상세"].lower()][Length] = self.df[self.ColumnsDict["상세"].lower()][Length][4000:]
            self.df[self.ColumnsDict["출처"].lower()][Length] = self.df[self.ColumnsDict["출처"].lower()][Length][4000:]
            self.df[self.ColumnsDict["사내"].lower()][Length] = self.df[self.ColumnsDict["사내"].lower()][Length][4000:]
            self.df[self.ColumnsDict["고객"].lower()][Length] = self.df[self.ColumnsDict["고객"].lower()][Length][4000:]  
            self.df[self.ColumnsDict["공급"].lower()][Length] = self.df[self.ColumnsDict["공급"].lower()][Length][4000:]
            self.df[self.ColumnsDict["경쟁"].lower()][Length] = self.df[self.ColumnsDict["경쟁"].lower()][Length][4000:]   
            self.df[self.ColumnsDict["대체"].lower()][Length] = self.df[self.ColumnsDict["대체"].lower()][Length][4000:]

        if News == True:
            for finan in self.Financial:
                self.df[finan.lower()] = np.nan
                self.df["entrp_reltn_tdngs_kind_cont"] = "Stock News"
            for Length in range(FirstRange):
                if len(str(self.df[self.ColumnsDict["날짜"].lower()][Length])) > 8:
                    date = str(self.df[self.ColumnsDict["날짜"].lower()][Length]).split()[:3]
                    if date[0] not in self.Month:
                        if len(date) < 8:
                            rlst = ["20221206", "20221205", "20221204"]
                            date = rlst[random.randrange(0, 2)]
                            self.df[self.ColumnsDict["날짜"].lower()][Length] = date
                        else:
                            date = str(self.df[self.ColumnsDict["날짜"].lower()][Length]).split()[3:6]
                            monthly = str(self.Month.index(date[0].replace("(", "")) + 1).rjust(2, "0")
                            date = date[2] + monthly + date[1].replace(",", "")
                            self.df[self.ColumnsDict["날짜"].lower()][Length] = date
                    else:
                        monthly = str(self.Month.index(date[0]) + 1).rjust(2, "0")
                        date = date[2] + monthly + date[1].replace(",", "")
                        self.df[self.ColumnsDict["날짜"].lower()][Length] = date
            if len(str(self.df[self.ColumnsDict["상세"].lower()][Length])) > 4000:
                try:
                    self.df[self.ColumnsDict["상세"].lower()][Length] = self.df[self.ColumnsDict["상세"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["출처"].lower()][Length] = self.df[self.ColumnsDict["출처"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["사내"].lower()][Length] = self.df[self.ColumnsDict["사내"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["고객"].lower()][Length] = self.df[self.ColumnsDict["고객"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["URL"].lower()][Length] = self.df[self.ColumnsDict["URL"].lower()][Length][:1000]  
                    self.df[self.ColumnsDict["공급"].lower()][Length] = self.df[self.ColumnsDict["공급"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["경쟁"].lower()][Length] = self.df[self.ColumnsDict["경쟁"].lower()][Length][:4000]   
                    self.df[self.ColumnsDict["대체"].lower()][Length] = self.df[self.ColumnsDict["대체"].lower()][Length][:4000]
                    self.df = self.df.append(self.df.iloc[Length:Length+1], ignore_index = True)
                    cnt += 1
                except:
                    pass
            else: pass


    def CheckNumeric(self):
        """
        숫자여부를 파악하고 숫자가 아닐 경우 삭제하고 숫자가 포함된 경우 숫자만 도출하는 함수 \n
        재무만 가능
        """
        numeric_match = re.compile('[^0-9]')
        for i in range(len(self.df)):
            for column in self.NumericList:
                result_lst = []
                try:
                    split_lst = self.df[column.lower()][i].split()
                    for idx in split_lst:
                        if numeric_match.match(idx) == None:
                            result_lst.append(re.sub(r'[^0-9]', '', idx))
                    if len(result_lst) >= 1:
                        if column == "STACNT_DT":
                            if 18000000 < int(result_lst[0]) < 21000000:
                                self.df[column.lower()][i] = result_lst[0]
                            else:
                                self.df[column.lower()][i] = ""    
                        else:
                            self.df[column.lower()][i] = result_lst[0]
                    else:
                        pass
                except:
                    pass

class Analysis:
    def __init__(self):
        """
        데이터를 분석하여 오류율을 엑셀형태로 저장하는 모듈
        """
        self.TableDefaultColumns ={
            "표준_영문컬럼명":[
            'keyval',
            'stock_mrkt_cd',
            'acplc_lngg_stock_mrkt_nm',
            'engls_stock_mrkt_nm',
            'hb_ntn_cd',
            'lstng_cd',
            'acplc_lngg_entrp_nm',
            'engls_entrp_nm',
            'ovrss_entrp_crprt_rgno',
            'ovrss_entrp_bsnsm_rgno',
            'fndtn_dt',
            'lstng_dt',
            'acplc_lngg_indst_gnnm',
            'engls_indstrsctrs_nm',
            'crrnc_sctin_cd',
            'accnn_yr',
            'reprt_kind_cd',
            'stacnt_dt',
            'cuass_amt',
            'nncrrnt_assts_amt',
            'cash_and_dpst_amt',
            'scrts_amt',
            'lon_bond_amt',
            'instm_fnc_assts_amt',
            'lease_assts_amt',
            'tpe_assts_amt',
            'etc_assts_amt',
            'assts_summ',
            'fltng_debt_amt',
            'nncrrnt_debt_amt',
            'cstdpslblits_amt',
            'cstdbt_amt',
            'etc_debt_amt',
            'debt_summ',
            'captl',
            'captl_srpl',
            'captl_mdtn_amt',
            'etc_inclsn_prlss_acttl_amt',
            'prft_srpl',
            'captl_summ',
            'debt_captl_summ_amt',
            'prsls',
            'sllng_prmpc_amt',
            'bsn_cost_amt',
            'bsn_prft_amt',
            'bsn_else_cost_amt',
            'ctax_cost_strbf_ntincmls_amt',
            'ctax_cost_strbf_cntntbs_plamt',
            'ctax_cost_amt',
            'cntntbs_prlss_ctax_cost_amt',
            'cntntbs_prft_amt',
            'dscnt_bsnss_prlss_amt',
            'thtrm_ntpf_amt',
            'bsn_acti_csflw_amt',
            'invsm_acti_cash_infl_amt',
            'fnnr_acti_cash_infl_amt',
            'cash_incre_amt',
            'bsis_cash_amt',
            'entrm_cash_amt',
            'debt_rate',
            'bsn_prft_rt_val',
            'prsls_incre_rt',
            'bsn_prft_incre_rt_val',
            'thtrm_ntpf_incre_rt_val',
            'entrp_yrmn_grrt_val',
            'entrp_reltn_tdngs_dt',
            'entrp_reltn_tdngs_kind_cont',
            'entrp_reltn_tdngs_subjc',
            'entrp_reltn_tdngs_cont_smmr',
            'entrp_reltn_tdngs_dtl_cont',
            'info_orgin_cont',
            'entrp_reltn_tdngs_url',
            'cmp_insd_reltn_info',
            'cstmr_reltn_info',
            'sppl_reltn_info',
            'cmptt_reltn_info',
            'sbst_goods_reltn_info',
            'opert_sctin_cd',
            'data_crtin_dt',
            'cntct_prces_stts_cd',
            'cntct_prces_dt'],
            "표준_한글컬렴명":[
            '키값',
            '주식시장코드',
            '현지언어주식시장명',
            '영문주식시장명',
            '헤브론스타국가코드',
            '상장코드',
            '현지언어기업명',
            '영문기업명',
            '해외기업법인등록번호',
            '해외기업사업자등록번호',
            '설립일자',
            '상장일자',
            '현지언어산업군명',
            '영문산업군명',
            '통화구분코드',
            '회계연도',
            '보고서종류코드',
            '결산일자',
            '유동자산금액',
            '비유동자산금액',
            '현금및예치금액',
            '유가증권금액',
            '대출채권금액',
            '할부금융자산금액',
            '리스자산금액',
            '유형자산금액',
            '기타자산금액',
            '자산총계',
            '유동부채금액',
            '비유동부채금액',
            '예수부채금액',
            '차입부채금액',
            '기타부채금액',
            '부채총계',
            '자본금',
            '자본잉여금',
            '자본조정금액',
            '기타포괄손익누계액',
            '이익잉여금',
            '자본총계',
            '부채자본총계액',
            '매출액',
            '매출원가금액',
            '영업비용금액',
            '영업이익금액',
            '영업외비용금액',
            '법인세비용차감전순손익금액',
            '법인세비용차감전계속사업손익금액',
            '법인세비용금액',
            '계속사업손익법인세비용금액',
            '계속사업이익금액',
            '중단사업손익금액',
            '당기순이익금액',
            '영업활동현금흐름금액',
            '투자활동현금유입금액',
            '재무활동현금유입금액',
            '현금증가금액',
            '기초현금금액',
            '기말현금금액',
            '부채비율',
            '영업이익율값',
            '매출액증가율',
            '영업이익증가율값',
            '당기순이익증가율값',
            '기업연평균성장률값',
            '기업관련소식날짜',
            '기업관련소식종류내용',
            '기업관련소식제목',
            '기업관련소식내용요약',
            '기업관련소식상세내용',
            '정보출처내용',
            '기업관련소식URL',
            '사내관련정보',
            '고객관련정보',
            '공급관련정보',
            '경쟁관련정보',
            '대체재관련정보',
            '작업구분코드',
            '데이터생성일자',
            '연계처리상태코드',
            '연계처리일자'
            ],
            "데이터타입":[
            'NUMERIC',
            'VARCHAR(6)',
            'VARCHAR(60)',
            'VARCHAR(60)',
            'VARCHAR(3)',
            'VARCHAR(12)',
            'VARCHAR(100)',
            'VARCHAR(100)',
            'VARCHAR(50)',
            'VARCHAR(50)',
            'VARCHAR(8)',
            'VARCHAR(8)',
            'VARCHAR(100)',
            'VARCHAR(100)',
            'VARCHAR(3)',
            'VARCHAR(4)',
            'VARCHAR(1)',
            'VARCHAR(8)',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'NUMERIC',
            'VARCHAR(4)',
            'NUMERIC',
            'VARCHAR(4)',
            'VARCHAR(4)',
            'VARCHAR(4)',
            'VARCHAR(8)',
            'VARCHAR(10)',
            'VARCHAR(100)',
            'VARCHAR(1000)',
            'VARCHAR(4000)',
            'VARCHAR(4000)',
            'VARCHAR(1000)',
            'VARCHAR(4000)',
            'VARCHAR(4000)',
            'VARCHAR(4000)',
            'VARCHAR(4000)',
            'VARCHAR(4000)',
            'VARCHAR(1)',
            'VARCHAR(8)',
            'VARCHAR(1)',
            'VARCHAR(8)']
        }
        self.TableDefault = {
            "한국 테이블명":[
                '일본 일반 기업정보 데이터',
                '홍콩 일반 기업정보 데이터',
                '말레이시아 일반 기업정보 데이터',
                '싱가포르 일반 기업정보 데이터',
                '태국 일반 기업정보 데이터',
                '베트남 일반 기업정보 데이터',
                '인도네시아 일반 기업정보 데이터',
                '인도 일반 기업정보 데이터',
                '미국 일반 기업정보 데이터',
                '캐나다 일반 기업정보 데이터',
                '콜롬비아 일반 기업정보 데이터',
                '멕시코 일반 기업정보 데이터',
                '네덜란드 일반 기업정보 데이터',
                '독일 일반 기업정보 데이터',
                '이탈리아 일반 기업정보 데이터',
                '프랑스 일반 기업정보 데이터',
                '영국 일반 기업정보 데이터',
                '호주 일반 기업정보 데이터',
                '스위스 일반 기업정보 데이터',
                '스페인 일반 기업정보 데이터',
                '한국 상장사 재무정보 데이터',
                '베트남 상장사 재무정보 데이터',
                '인도네시아 상장사 재무정보 데이터',
                '미국 상장사 재무정보 데이터',
                '일본 상장사 재무정보 데이터',
                '홍콩 상장사 재무정보 데이터',
                '말레이시아 상장사 재무정보 데이터',
                '싱가포르 상장사 재무정보 데이터',
                '태국 상장사 재무정보 데이터',
                '인도 상장사 재무정보 데이터',
                '캐나다 상장사 재무정보 데이터',
                '콜롬비아 상장사 재무정보 데이터',
                '멕시코 상장사 재무정보 데이터',
                '네덜란드 상장사 재무정보 데이터',
                '독일 상장사 재무정보 데이터',
                '이탈리아 상장사 재무정보 데이터',
                '프랑스 상장사 재무정보 데이터',
                '영국 상장사 재무정보 데이터',
                '호주 상장사 재무정보 데이터',
                '스위스 상장사 재무정보 데이터',
                '스페인 상장사 재무정보 데이터'],
            "영문 테이블명":[
                'tb_hb_jpn_egnin_m',
                'tb_hb_hkg_egnin_m',
                'tb_hb_mys_egnin_m',
                'tb_hb_sgp_egnin_m',
                'tb_hb_tha_egnin_m',
                'tb_hb_vnm_egnin_m',
                'tb_hb_idn_egnin_m',
                'tb_hb_ind_egnin_m',
                'tb_hb_usa_egnin_m',
                'tb_hb_can_egnin_m',
                'tb_hb_col_egnin_m',
                'tb_hb_mex_egnin_m',
                'tb_hb_nld_egnin_m',
                'tb_hb_deu_egnin_m',
                'tb_hb_ita_egnin_m',
                'tb_hb_fra_egnin_m',
                'tb_hb_gbr_egnin_m',
                'tb_hb_aus_egnin_m',
                'tb_hb_che_egnin_m',
                'tb_hb_esp_egnin_m',
                'tb_hb_kor_plcfi_d',
                'tb_hb_vnm_plcfi_d',
                'tb_hb_idn_plcfi_d',
                'tb_hb_usa_plcfi_d',
                'tb_hb_jpn_plcfi_d',
                'tb_hb_hkg_plcfi_d',
                'tb_hb_mys_plcfi_d',
                'tb_hb_sgp_plcfi_d',
                'tb_hb_tha_plcfi_d',
                'tb_hb_ind_plcfi_d',
                'tb_hb_can_plcfi_d',
                'tb_hb_col_plcfi_d',
                'tb_hb_mex_plcfi_d',
                'tb_hb_nld_plcfi_d',
                'tb_hb_deu_plcfi_d',
                'tb_hb_ita_plcfi_d',
                'tb_hb_fra_plcfi_d',
                'tb_hb_gbr_plcfi_d',
                'tb_hb_aus_plcfi_d',
                'tb_hb_che_plcfi_d',
                'tb_hb_esp_plcfi_d']
        }
        self.DefaultTables = pd.DataFrame(self.TableDefaultColumns)
        self.DefaultColumns = pd.DataFrame(self.TableDefault)
        self.ColumnDF = self.DefaultColumns.T
        self.ColumnDF = self.ColumnDF.rename(columns=self.ColumnDF.iloc[0])
        self.ColumnDF = self.ColumnDF.drop(self.ColumnDF.index[0])
        self.ExceptList = ["stock_mrkt_cd", "acplc_lngg_stock_mrkt_nm", "engls_stock_mrkt_nm", "hb_ntn_cd", "crrnc_sctin_cd", 
                    "accnn_yr", "reprt_kind_cd","stacnt_dt", "opert_sctin_cd", "data_crtin_dt",
                    "cntct_prces_stts_cd", "cntct_prces_dt"]
        self.count = 0
        self.CheckDF = pd.DataFrame(columns=["번호", "시스템", "업무구분", "검증구분", "테이블명", "테이블ID", "컬럼명", "컬럼ID",
                                        "pk", "FK", "NN", "데이터타입", "도메인소분류",
                                        "도메인명", "전체건수", "오류건수", "오류율", "점검일시", "오류데이터"])
        self.DateList = list(self.DefaultTables[self.DefaultTables["데이터타입"] == "VARCHAR(8)"]["표준_영문컬럼명"].values)
        self.CheckDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def read_excel(self, path):
        """
        path에 excel 주소를 입력하세요.
        """
        self.df = pd.read_excel(path)
        Table = path.split(".")[0]
        self.TableName = str(self.DefaultTables[self.DefaultTables["영문 테이블명"] == Table]["한국 테이블명"])
        self.TableID = str(Table)

    def read_csv(self, path):
        """
        path에 csv 주소를 입력하세요.
        """
        self.df = pd.read_csv(path)
        Table = path.split(".")[0]
        self.TableName = str(self.DefaultTables[self.DefaultTables["영문 테이블명"] == Table]["한국 테이블명"])
        self.TableID = str(Table)

    def Fail(self, column, Failed):
        """
        오류난 데이터를 입력하는 함수
        column = 오류난 컬럼
        Failed = 오류난 데이터
        """ 
        ColumnName = self.ColumnDF[column].values[0] # 컬럼명
        ColumnDataType = self.ColumnDF[column].values[1] # 데이터타입
        FailedDict = {
                        "번호" : self.count,
                        "시스템" : "공통",
                        "업무구분" : "헤브론스타",
                        "검증구분" : self.verification,
                        "테이블명" : self.TableName,
                        "테이블ID" : self.TableID,
                        "컬럼명" : self.ColumnDF[column].values[0],
                        "컬럼ID" : column,
                        "pk" : "N",
                        "FK" : "N",
                        "NN" : "N",
                        "데이터타입" : self.ColumnDF[column].values[1],
                        "도메인소분류" : "",
                        "도메인명" : "",
                        "전체건수" : len(self.df),
                        "오류건수" : len(self.df[self.df[column] == Failed]),
                        "오류율" : round(len(self.df[self.df[column] == Failed]) / len(self.df), 3),
                        "점검일시" : self.CheckDate,
                        "오류데이터" : Failed
                    }
        return FailedDict

    def CheckDate_Duplicate(self):
        """
        날짜와 동시에 중복여부를 확인하는 함수.
        """
        self.df.columns = list(self.DefaultColumns["표준_영문컬럼명"].values[:77])        
        for column in self.df.columns: # 컬럼ID
            for length in range(len(self.df[column])):
                self.verification = "날짜"
                if column in self.DateList:
                    try: pd.to_datetime(str(self.df[column][length]), format='%Y%m%d')
                    except:
                        self.count += 1
                        Failed = self.df[column][length]
                        Returned = self.Fail(column, Failed)
                        self.Success = self.Success.append(Returned, ignore_index=True)
            if column in self.ExceptList: continue
            else:
                for cwt, idx in zip(self.df[column].value_counts(), self.df[column].value_counts().index):
                    per = cwt / len(self.df)
                    self.verification = "중복"
                    if per >= 0.05:
                        self.count += 1
                        Failed = idx
                        Returned = self.Fail(column, Failed)
                        self.Success = self.Success.append(Returned, ignore_index=True)

    def CheckNumber(self, phone_columns, cop_columns):
        """
        전화번호의 컬럼들을 확인하는 함수
        """
        PN_1 = re.compile("\d{2,3}-\d{3,4}-\d{4}$") # 전화번호 예시 1 000-0000-0000
        PN_2 = re.compile("\(\d{2,3}\)-\d{3,4}-\d{3,4}$") # 전화번호 예시 2 (000)-0000-0000
        PN_3 = re.compile("\d{1,2} \d{7,8}$") # 전화번호 예시 3 00 00000000
        BS_1 = re.compile("\d{1,2}-\d{7,8}$") # 법인번호 예시 1 00-00000000
        for column in phone_columns:
            for idx in range(len(self.df[column])):
                PN_Check_1 = PN_1.search(self.df[column][idx])
                PN_Check_2 = PN_2.search(self.df[column][idx])
                PN_Check_3 = PN_3.search(self.df[column][idx])
                if PN_Check_1 or PN_Check_2 or PN_Check_3:
                    pass
                else:
                    self.verification = "전화번호오류"
                    Failed = self.df[column][idx]
                    Returned = self.Fail(column, Failed)
                    self.Success = self.CheckDF.append(Returned, ignore_index=True)
        for column in cop_columns:
            for idx in range(len(self.df[column])):
                BS_Check_1 = BS_1.search(self.df[column][idx])
                if BS_Check_1:
                    pass
                else:
                    self.verification = "법인번호오류"
                    Failed = self.df[column][idx]
                    Returned = self.Fail(column, Failed)
                    self.Success = self.CheckDF.append(Returned, ignore_index=True)

    def change_columns(self, df):
        """
        표준 한글컬럼명으로 수집된 데이터 영문 컬럼명으로 바꿔주는 함수
        """
        df.columns = [self.TableDefaultColumns["표준_영문컬럼명"][self.TableDefaultColumns["표준_한글컬렴명"].index(col)] if col in self.TableDefaultColumns["표준_한글컬렴명"] else col for col in df.columns]
        return df