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

class datos_extract:
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
        df = pd.read_csv(path)
        AppendDict = {
            "hb_ntn_cd" : "COL",
            "acplc_lngg_ntn_nm" : "Colombia",
            "engls_ntn_nm" : "Colombia",
            "ntn_lngg_cd_val" : "COL",
            "acplc_lngg_lngg_nm" : "Colombia",
            "engls_lngg_nm" : "Colombia",
            "acplc_lngg_entrp_nm" : None,
            "engls_entrp_nm" : None,
            "acplc_lngg_oln_intrd_cont" : None,
            "acplc_lngg_entrp_intrd_cont" : None,
            "engls_oln_intrd_cont" : None,
            "engls_entrp_intrd_cont" : None,
            "rprsn_email" : None,
            "acplc_lngg_entrp_addr" : None,
            "acplc_lngg_entrp_dtadd" : None,
            "engls_entrp_addr" : None,
            "engls_entrp_dtadd" : None,
            "acplc_lngg_ceo_nm" : None, 
            "engls_ceo_nm" : None,
            "fndtn_dt" : None
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
        function to store data
        """
        self.DefaultDataFrame.to_excel("tb_hb_col_egnin_m.xlsx", index=False)