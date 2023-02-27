import os 
import re
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
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 dart_extract입니다. \n
        api_key()는 api key를 알려주는 함수입니다. \n
        extract_finstate()은 데이터를 추출하는 함수입니다. \n
        load_finstate()은 데이터를 저장하는 함수입니다.
        """)

class dart_extract:
    def __init__(self, Path):
        """
        data에 http://data.krx.co.kr/contents/MDC/MDI/mdiLoader 에서 \n
        다운받은 데이터 파일 경로를 입력하세요.
        """
        self.Columns = [
            "키값",
            "주식시장코드",
            "현지언어주식시장명",
            "영문주식시장명",
            "헤브론스타국가코드",
            "상장코드",
            "현지언어기업명",
            "영문기업명",
            "법인등록번호",
            "사업자등록번호",
            "설립일자",
            "상장일자",
            "현지언어산업군명",
            "영문산업군명",
            "통화구분코드",
            "회계연도",
            "보고서종류코드",
            "결산일자",
            "유동자산금액",
            "비유동자산금액",
            "현금및예치금액",
            "유가증권금액",
            "대출채권금액",
            "할부금융자산금액",
            "리스자산금액",
            "유형자산금액",
            "기타자산금액",
            "자산총계",
            "유동부채금액",
            "비유동부채금액",
            "예수부채금액",
            "차입부채금액",
            "기타부채금액",
            "부채총계",
            "자본금",
            "자본잉여금",
            "자본조정금액",
            "기타포괄손익누계액",
            "이익잉여금",
            "자본총계",
            "부채자본총계액",
            "매출액",
            "매출원가금액",
            "영업비용금액",
            "영업이익금액",
            "영업외비용금액",
            "법인세비용차감전순손익금액",
            "법인세비용차감전계속사업손익금액",
            "법인세비용금액",
            "계속사업손익법인세비용금액",
            "계속사업이익금액",
            "중단사업손익금액",
            "당기순이익금액",
            "영업활동현금흐름금액",
            "투자활동현금유입금액",
            "재무활동현금유입금액",
            "현금증가금액",
            "기초현금금액",
            "기말현금금액",
            "부채비율",
            "영업이익율값",
            "매출액증가율",
            "영업이익증가율값",
            "당기순이익증가율값",
            "기업연평균성장률값",
            "기업관련소식날짜",
            "기업관련소식종류내용",
            "기업관련소식제목",
            "기업관련소식내용요약",
            "기업관련소식상세내용",
            "정보출처내용",
            "기업관련소식URL",
            "사내관련정보",
            "고객관련정보",
            "공급관련정보",
            "경쟁관련정보",
            "대체재관련정보",
            "작업구분코드",
            "데이터생성일자",
            "연계처리상태코드",
            "연계처리일자"]
        self.cleansed_finstats = pd.DataFrame(columns= self.Columns)
        if Path.split(".")[2] == "xlsx":
            self.df = pd.read_excel(Path)
        elif Path.split(".")[2] == "csv":
            self.df = pd.read_csv(Path)
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
        reprt에 ["11013", "11012", "11014"] 값을 입력하세요. \n
        api key에 api_key()를 통해 4개 중 하나를 선택하여 넣으세요. 
        """
        dart = OpenDartReader(api_key)
        success_num = 0
        failed_num = 0
        success_columns = []
        add_index = []
        try:
            founded_finstats = pd.DataFrame(dart.finstate_all(name, 2022, reprt_code = reprt).set_index("account_nm").T)
        except:
            self.failed_companies.append(name)
            return f"{name}기업의 재무정보를 찾을 수 없습니다."
        company_info1 = dart.company(name)
        # cleansed_finstats['키값'] = np.nan
        success_columns.append('주식시장코드')
        success_columns.append('현지언어주식시장명')
        success_columns.append('영문주식시장명')
        success_columns.append('헤브론스타국가코드')
        success_columns.append('상장코드')
        success_columns.append('현지언어기업명')
        success_columns.append('영문기업명')
        success_columns.append('법인등록번호')
        success_columns.append('사업자등록번호')
        success_columns.append('설립일자')
        # 상장일자
        success_columns.append('현지언어산업군명')
        success_columns.append('통화구분코드')
        success_columns.append('회계연도')
        success_columns.append('보고서종류코드')
        success_columns.append('결산일자')
        시장구분 = str(self.df[self.df["한글 종목약명"] == name]["시장구분"]).split()[1]
        add_index.append(시장구분)
        if 시장구분 == 'KOSDAQ':
            add_index.append("코스닥시장")
        elif 시장구분 == 'KOSPI':
            add_index.append("유가증권시장")
        elif 시장구분 == 'KONEX':
            add_index.append("코넥스시장")
        try:
            add_index.append(시장구분)
            add_index.append("KOR")
            add_index.append(company_info1['stock_code'])
            add_index.append(company_info1["corp_name"])
            add_index.append(company_info1['corp_name_eng'])
            add_index.append(company_info1['jurir_no'])
            add_index.append(company_info1['bizr_no'])
            add_index.append(company_info1['est_dt'])
            add_index.append(company_info1['induty_code'])
            # cleansed_finstats['영문언어산업군명'] = 업종명
            add_index.append("KRW")
            add_index.append("2022")
        except:
            return
        try:
            if '분기' in founded_finstats["유동자산"]:
                add_index.append("Q")
            else:
                add_index.append("A")
        except:
            pass
        try:
            add_index.append(founded_finstats['유동자산'].loc['rcept_no'][:8])
        except:
            add_index.append("19991118")
            
        for default_idx, idx in zip(self.Columns["기준"], self.Columns["대안1"]):
            if idx == "없음":
                pass
            try:
                add_index.append(founded_finstats[idx].loc["thstrm_amount"])
                # print("해당 컬럼은 존재합니다.")
                success_columns.append(default_idx)
                success_num += 1
            except KeyError:
                # print("해당 컬럼은 존재하지 않습니다.")
                failed_num += 1
        while len(success_columns) > len(add_index):
            if len(success_columns) == len(add_index): break
            else: add_index.append("0")
        complete_df = pd.DataFrame([tuple(add_index)], columns=success_columns)
        cleansed_finstats = cleansed_finstats.append(complete_df, ignore_index=True)
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