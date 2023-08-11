import socket
from _thread import *
import pandas as pd
import time
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from tqdm import tqdm
import urllib.request

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        The function is described below. \n
        The main class in the library is extract().\n
        extract() collects data from wsj.\n
        collect() imports only OTC companies of the data collected.
        """)

class wsj:
    def __init__(self, usa_indexes):
        """
        please input usa_indexes excel file \n
        usa_indexes in wsj homepage
        """
        self.df = pd.read_excel(usa_indexes)
        self.OTC_df = self.df[self.df["Exchange"] == "OOTC"]

        self.lst = ["balance-sheet", "cash-flow", "income-statement"]
        self.cf_df = pd.DataFrame()
        self.bs_df = pd.DataFrame()
        self.is_df = pd.DataFrame()
        self.failed_lst = []
        self.cnt = 0
        self.number = 0

    def extract(self, symbols_name, type, name):
        url = f"https://www.wsj.com/market-data/quotes/{symbols_name}/financials/annual/{idx}"
        try: response = urlopen(url, timeout=5)
        except urllib.error.URLError as e:
            if isinstance(e.reason, socket.timeout):
                print("time out.")
                time.sleep(3)
                self.extract(symbols_name, type, name)
        try:
            urlTicker = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            readTicker = urlopen(urlTicker).read()
            soup = BeautifulSoup(readTicker, 'lxml')
            table = soup.find_all('table')
        except: return
        try: df = pd.read_html(str(table))[0]
        except: return
        # df["name"] = name
        if type == "balance-sheet":
            try: df.to_excel(f"./balance_sheet/bs_df_{symbols_name}.xlsx", index=False)
            except: 
                self.failed_lst.append(symbols_name)
                print(f"{name} is Failed")
            # self.bs_df = self.bs_df.append(df)
        elif type == "cash-flow": 
            try: df.to_excel(f"./cash_flow/cs_df_{symbols_name}.xlsx", index=False)
            except:
                self.failed_lst.append(symbols_name)
                print(f"{name} is Failed")
            # self.cf_df = self.cf_df.append(df)
        elif type == "income-statement": 
            try: df.to_excel(f"./income_statement/is_df_{symbols_name}.xlsx", index=False)
            except:
                self.failed_lst.append(symbols_name)
                print(f"{name} is Failed")
            # self.is_df = self.is_df.append(df)
        print(f"data extracted. {name}")

    def collect(self):
        for name in tqdm(list(self.OTC_df["Name"])[990:]):
            symbols_name = name.split("(")[-1].replace(")","")
            for type in self.lst:
                self.extract(symbols_name, type, name)