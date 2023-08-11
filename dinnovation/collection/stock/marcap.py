import pandas as pd
from tqdm import tqdm

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is MARCAP. \n
        install() is a function that informs the marcap data github address. \n
        collect() is a function that extracts data.
        """)

class MARCAP:

    def __init__(self, ticker_lst) -> list:
        """
        please input ticker_lst
        """
        self.exist_lst = []
        self.ticker_lst = ticker_lst
    
    def install():
        return '!git clone "https://github.com/FinanceData/marcap.git" marcap'
    
    def collect(self):
        from marcap import marcap_data
        for name in tqdm(self.ticker_lst):
            try:
                df = marcap_data('2018-01-01', '2023-01-01', code='005930') 
                df = df.reset_index()
                df.to_csv("marcap_data/" + name + "_5yr.csv", index=False)
            except:
                self.exist_lst.append(name)
                print(name + " data does not exist")
                pass