import pandas as pd

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 datos_extact입니다. \n
        make()는 데이터를 가공하는 함수입니다. \n
        load()은 데이터를 저장하는 함수입니다.
        """)

class datos_extract:
    def __init__(self, path):
        """
        path에 TableDefaultColumns 엑셀을 넣어주세요.
        """
        self.DefaultDataFrame = pd.DataFrame(columns = list(pd.read_excel(path, sheet_name="일반_columns")["표준_영문컬럼명"]))

    def make(self, path):
        """
        데이터를 표준화하는 함수
        path에 Datos에서 다운받은 엑셀을 넣어주세요.
        """
        df = pd.read_csv(path)
        AppendDict = {
            "hb_ntn_cd" : "COL",
            "acplc_lngg_ntn_nm" : "Colombia",
            "engls_ntn_nm" : "Colombia",
            "ntn_lngg_cd_val" : "COL",
            "acplc_lngg_lngg_nm" : "Colombia",
            "engls_lngg_nm" : "Colombia",
            "acplc_lngg_entrp_nm" : None, # 기업명
            "engls_entrp_nm" : None, # 기업명
            "acplc_lngg_oln_intrd_cont" : None, # 한줄소개
            "acplc_lngg_entrp_intrd_cont" : None, # 기업소개
            "engls_oln_intrd_cont" : None, # 영문 한줄소개
            "engls_entrp_intrd_cont" : None, # 영문 기업소개
            "rprsn_email" : None, # 이메일
            "acplc_lngg_entrp_addr" : None, # 기업 주소
            "acplc_lngg_entrp_dtadd" : None, # 기업 상세 주소
            "engls_entrp_addr" : None, # 기업 주소
            "engls_entrp_dtadd" : None, # 기업 상세 주소
            "acplc_lngg_ceo_nm" : None, # 대표
            "engls_ceo_nm" : None, # 대표
            "fndtn_dt" : None # 설립일자
        }
        company_email = list(df["EMAIL-COMERCIAL"])
        company_ceo = list(df["NOM-REP-LEGAL"])
        company_address = list(df["DIR-COMERCIAL"])
        company_name = list(df["RAZON SOCIAL"])
        AppendDict["acplc_lngg_entrp_nm"] = company_name
        AppendDict["engls_entrp_nm"] = company_name
        AppendDict["acplc_lngg_ceo_nm"] = company_ceo
        AppendDict["engls_ceo_nm"] = company_ceo
        AppendDict["acplc_lngg_entrp_addr"] = company_address
        AppendDict["acplc_lngg_entrp_dtadd"] = company_address
        AppendDict["engls_entrp_addr"] = company_address
        AppendDict["engls_entrp_dtadd"] = company_address
        AppendDict["rprsn_email"] = company_email
        AppendDataFrame = pd.DataFrame(AppendDict)
        self.DefaultDataFrame = self.DefaultDataFrame.append(AppendDataFrame, ignore_index=True)

    def load(self):
        """
        데이터를 저장하는 함수
        """
        self.DefaultDataFrame.to_excel("tb_hb_col_egnin_m.xlsx", index=False)