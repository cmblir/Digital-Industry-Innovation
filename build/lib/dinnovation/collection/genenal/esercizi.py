import pandas as pd

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is datos_extact. \n
        make() is a function that processes data. \n
        load() is a function that stores data.
        """)

class esercizi_extract:
    def __init__(self, path):
        """
        Please put TableDefaultColumns Excel in the path.
        """
        self.DefaultDataFrame = pd.DataFrame(columns = list(pd.read_excel(path, sheet_name="일반_columns")["표준_영문컬럼명"]))

    def make(self, path):
        """
        Function to standardize data
        Put the excel downloaded from Datos in the path.
        """
        df = pd.read_excel(path)
        AppendDict = {
            "hb_ntn_cd" : "ITA",
            "acplc_lngg_ntn_nm" : "Italy",
            "engls_ntn_nm" : "Italy",
            "ntn_lngg_cd_val" : "ITA",
            "acplc_lngg_lngg_nm" : "Italy",
            "engls_lngg_nm" : "Italy",
            "acplc_lngg_entrp_nm" : None,
            "engls_entrp_nm" : None,
            "acplc_lngg_oln_intrd_cont" : None,
            "acplc_lngg_entrp_intrd_cont" : None,
            "engls_oln_intrd_cont" : None,
            "engls_entrp_intrd_cont" : None,
            "entrp_rprsn_tlno" : None,
            "acplc_lngg_entrp_addr" : None,
            "acplc_lngg_entrp_dtadd" : None, 
            "engls_entrp_addr" : None,
            "engls_entrp_dtadd" : None,
            "acplc_lngg_ceo_nm" : None,
            "engls_ceo_nm" : None,
            "fndtn_dt" : None 
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