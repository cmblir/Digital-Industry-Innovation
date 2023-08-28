import pandas as pd
from .constants import const
from tqdm import tqdm
import OpenDartReader

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is dart_extract. \n
        api_key() is a function that informs the api key. \n
        extract_finstate() is a function that extracts data. \n
        load_finstate() is a function that saves data.
        """)

class dart_extract:
    def __init__(self, Path):
        """
        Download all items from data at http://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd. \n
        Enter the downloaded data file path.
        """
        self.Columns = const.korea_financial_column
        self.cleansed_finstats = pd.DataFrame(columns= self.Columns)
        if Path.split(".")[-1] == "xlsx": self.df = pd.read_excel(Path)
        elif Path.split(".")[-1] == "csv": self.df = pd.read_csv(Path, encoding='cp949')
        self.listed_companies = self.df[self.df['주식종류']=='보통주']["한글 종목약명"]
        self.failed_companies = []
        
    def extract_finstate(self, name, reprt, api_key):
        """
        For name, enter the company name through the received data. \n
        Enter the values ["11013", "11012", "11014", "11011"] in reprt. \n
        Quarterly, Semiannually, Thirdly, Annually, in order.\n
        Select and insert one of the four into the api key through api_key().
        """
        dart = OpenDartReader(api_key)
        success_num = 0
        failed_num = 0
        self.success_columns = []
        self.add_index = []
        try:
            self.founded_finstats = pd.DataFrame(dart.finstate_all(name, 2022, reprt_code = reprt).set_index("account_nm").T)
        except:
            self.failed_companies.append(name)
            return f"{name}기업의 재무정보를 찾을 수 없습니다."
        self.company_info1 = dart.company(name)
        # cleansed_finstats['키값'] = np.nan
        self.success_columns.append('주식시장코드')
        self.success_columns.append('현지언어주식시장명')
        self.success_columns.append('영문주식시장명')
        self.success_columns.append('헤브론스타국가코드')
        self.success_columns.append('상장코드')
        self.success_columns.append('현지언어기업명')
        self.success_columns.append('영문기업명')
        self.success_columns.append('법인등록번호')
        self.success_columns.append('사업자등록번호')
        self.success_columns.append('설립일자')
        self.success_columns.append('현지언어산업군명')
        self.success_columns.append('통화구분코드')
        self.success_columns.append('회계연도')
        self.success_columns.append('보고서종류코드')
        self.success_columns.append('결산일자')
        market_segmentation = str(self.df[self.df["한글 종목약명"] == name]["시장구분"]).split()[1]
        self.add_index.append(market_segmentation)
        if market_segmentation == 'KOSDAQ':
            self.add_index.append("코스닥시장")
        elif market_segmentation == 'KOSPI':
            self.add_index.append("유가증권시장")
        elif market_segmentation == 'KONEX':
            self.add_index.append("코넥스시장")
        try:
            self.add_index.append(market_segmentation)
            self.add_index.append("KOR")
            self.add_index.append(self.company_info1['stock_code'])
            self.add_index.append(self.company_info1["corp_name"])
            self.add_index.append(self.company_info1['corp_name_eng'])
            self.add_index.append(self.company_info1['jurir_no'])
            self.add_index.append(self.company_info1['bizr_no'])
            self.add_index.append(self.company_info1['est_dt'])
            self.add_index.append(self.company_info1['induty_code'])
            self.add_index.append("KRW")
            self.add_index.append("2022")
        except:
            return
        try:
            if '분기' in self.founded_finstats["유동자산"]:
                self.add_index.append("Q")
            else:
                self.add_index.append("A")
        except:
            pass
        try:
            self.add_index.append(self.founded_finstats['유동자산'].loc['rcept_no'][:8])
        except:
            self.add_index.append("19991118")
            
        for idx in self.Columns:
            if idx == "없음":
                pass
            try:
                self.add_index.append(self.founded_finstats[idx].loc["thstrm_amount"])
                self.success_columns.append(idx)
                success_num += 1
            except KeyError:
                failed_num += 1
        while len(self.success_columns) > len(self.add_index):
            if len(self.success_columns) == len(self.add_index): break
            else: self.add_index.append("0")
        self.complete_df = pd.DataFrame([tuple(self.add_index)], columns=self.success_columns)
        self.cleansed_finstats = self.cleansed_finstats.append(self.complete_df, ignore_index=True)

    def load_finstats(self, api_key):
        """
        Function to store data \n
        Select and insert one of the four into the api key through api_key().
        """
        for name in tqdm(self.listed_companies):
            for num in ["11013", "11012", "11014"]:
                self.extract_finstate(name, num, api_key)
        self.cleansed_finstats.to_excel("KOREA.xlsx", index=False)