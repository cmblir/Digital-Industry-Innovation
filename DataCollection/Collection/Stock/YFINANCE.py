import yfinance as yf
import psycopg2
import pandas as pd
from tqdm import tqdm

class YFINANCE:
    def __init__(self, country):
        self.conn = psycopg2.connect(
            host = "218.39.214.98",
            database = "nia",
            user = "nia",
            password = "nia"
        )
        self.query = "SELECT * FROM tb_hb_{country}_plcfi_d"
        self.df = pd.read_sql(self.query, self.conn)
        self.company_lst = [i for i in list(self.df["lstng_cd"].unique()) if i is not None]
    
    def collect(self, path):
        for name in self.company_lst:
            ticker = yf.Ticker(name)
            hist = ticker.history(period="1wk", start="2018-01-01")
            hist.to_excel(f"{path}/"+ name + "_5yr.xlsx", index= False)