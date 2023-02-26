import pandas as pd

class esercizi_extract:
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
        df = pd.read_excel(path)
        AppendDict = {
            "hb_ntn_cd" : "ITA",
            "acplc_lngg_ntn_nm" : "Italy",
            "engls_ntn_nm" : "Italy",
            "ntn_lngg_cd_val" : "ITA",
            "acplc_lngg_lngg_nm" : "Italy",
            "engls_lngg_nm" : "Italy",
            "acplc_lngg_entrp_nm" : None, # 기업명
            "engls_entrp_nm" : None, # 기업명
            "acplc_lngg_oln_intrd_cont" : None, # 한줄소개
            "acplc_lngg_entrp_intrd_cont" : None, # 기업소개
            "engls_oln_intrd_cont" : None, # 영문 한줄소개
            "engls_entrp_intrd_cont" : None, # 영문 기업소개
            "entrp_rprsn_tlno" : None, # 전화번호
            "acplc_lngg_entrp_addr" : None, # 기업 주소
            "acplc_lngg_entrp_dtadd" : None, # 기업 상세 주소
            "engls_entrp_addr" : None, # 기업 주소
            "engls_entrp_dtadd" : None, # 기업 상세 주소
            "acplc_lngg_ceo_nm" : None, # 대표
            "engls_ceo_nm" : None, # 대표
            "fndtn_dt" : None # 설립일자
        }
        AppendDict["acplc_lngg_entrp_nm"] = list(df["COMPANY NAME"])
        AppendDict["engls_entrp_nm"] = list(df["COMPANY NAME"])
        AppendDict["acplc_lngg_ceo_nm"] = list(df["LEG. REPRESENTATIVE"])
        AppendDict["engls_ceo_nm"] = list(df["LEG. REPRESENTATIVE"])
        AppendDict["fndtn_dt"] = list(df["COMMUNICATION DATE"])
        AppendDict["acplc_lngg_entrp_addr"] = list(df["LOCATION"])
        AppendDict["engls_entrp_addr"] = list(df["LOCATION"])
        AppendDataFrame = pd.DataFrame(AppendDict)
        self.DefaultDataFrame = self.DefaultDataFrame.append(AppendDataFrame, ignore_index=True)

    def load(self):
        """
        데이터를 저장하는 함수
        """
        self.DefaultDataFrame.to_excel("tb_hb_ita_egnin_m.xlsx", index = False)