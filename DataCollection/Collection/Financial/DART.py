import dart_constants as constants
import pandas as pd
from datetime import datetime
import numpy as np
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
        data에 http://data.krx.co.kr/contents/MDC/MDI/mdiLoader 에서 전종목을 다운받으세요. \n
        다운받은 데이터 파일 경로를 입력하세요.
        """
        self.Columns = constants.Columns
        self.cleansed_finstats = pd.DataFrame(columns= self.Columns)
        if Path.split(".")[-1] == "xlsx": self.df = pd.read_excel(Path)
        elif Path.split(".")[-1] == "csv": self.df = pd.read_csv(Path, encoding='cp949')
        self.listed_companies = self.df[self.df['주식종류']=='보통주']["한글 종목약명"]
        self.failed_companies = []

    def api_key(self):
        """
        api_key 리스트는 아래와 같습니다.
        """
        print("""
        api_key1 ='7db60698e63bd621a006025500fe0436ee65cb47' \n
        api_key2 = 'fe6d5fea0efcad8f5ce367a096624b9a2213bc0d' \n
        api_key3 = '2f4e65c96c0399bb2a76d81e45889ac98395998d' \n
        api_key4 = '3b85edb9e9282fe585038018c3556d9d4b7fb490'
        """)
        
    def extract_finstate(self, name, reprt, api_key):
        """
        name은 받은 데이털르 통해서 기업명을 넣으세요. \n
        reprt에 ["11013", "11012", "11014", "11011"] 값을 입력하세요. \n
        순서대로 1분기, 반기, 3분기, 연간입니다.\n
        api key에 api_key()를 통해 4개 중 하나를 선택하여 넣으세요. 
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
        # 상장일자
        self.success_columns.append('현지언어산업군명')
        self.success_columns.append('통화구분코드')
        self.success_columns.append('회계연도')
        self.success_columns.append('보고서종류코드')
        self.success_columns.append('결산일자')
        시장구분 = str(self.df[self.df["한글 종목약명"] == name]["시장구분"]).split()[1]
        self.add_index.append(시장구분)
        if 시장구분 == 'KOSDAQ':
            self.add_index.append("코스닥시장")
        elif 시장구분 == 'KOSPI':
            self.add_index.append("유가증권시장")
        elif 시장구분 == 'KONEX':
            self.add_index.append("코넥스시장")
        try:
            self.add_index.append(시장구분)
            self.add_index.append("KOR")
            self.add_index.append(self.company_info1['stock_code'])
            self.add_index.append(self.company_info1["corp_name"])
            self.add_index.append(self.company_info1['corp_name_eng'])
            self.add_index.append(self.company_info1['jurir_no'])
            self.add_index.append(self.company_info1['bizr_no'])
            self.add_index.append(self.company_info1['est_dt'])
            self.add_index.append(self.company_info1['induty_code'])
            # cleansed_finstats['영문언어산업군명'] = 업종명
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
                # print("해당 컬럼은 존재합니다.")
                self.success_columns.append(idx)
                success_num += 1
            except KeyError:
                # print("해당 컬럼은 존재하지 않습니다.")
                failed_num += 1
        while len(self.success_columns) > len(self.add_index):
            if len(self.success_columns) == len(self.add_index): break
            else: self.add_index.append("0")
        self.complete_df = pd.DataFrame([tuple(self.add_index)], columns=self.success_columns)
        self.cleansed_finstats = self.cleansed_finstats.append(self.complete_df, ignore_index=True)
        # print(f"success {name}_{reprt}")

    def load_finstats(self, api_key):
        """
        데이터를 저장하는 함수 \n
        api key에 api_key()를 통해 4개 중 하나를 선택하여 넣으세요. 
        """
        for name in tqdm(self.listed_companies):
            for num in ["11013", "11012", "11014"]:
                self.extract_finstate(name, num, api_key)
        self.cleansed_finstats.to_excel("KOREA_1Q-3Q.xlsx", index=False)