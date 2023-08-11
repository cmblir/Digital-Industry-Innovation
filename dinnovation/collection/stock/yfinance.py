import yfinance as yf
import psycopg2
import pandas as pd

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        The yfinance library collects market cap data. \n
        collect() is a function that collects data from shareoutstanding sites. \n
        """)

class YFINANCE:
    def __init__(self, country, host, database, user, password):
        conn = psycopg2.connect(
                host = host,
                database = database,
                user = user,
                password = password
                )
        self.query = f"SELECT * FROM tb_hb_{country}_plcfi_d"
        self.df = pd.read_sql(self.query, self.conn)
        self.company_lst = [i for i in list(self.df["lstng_cd"].unique()) if i is not None]
    
    def collect(self, path):
        for name in self.company_lst:
            ticker = yf.Ticker(name)
            hist = ticker.history(period="1d", start="2018-01-01")
            hist = hist.reset_index()
            hist["Date"] = hist["Date"].astype(str)
            hist.to_excel(f"{path}/"+ name + "_5yr.xlsx", index= False)