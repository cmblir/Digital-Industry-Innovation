import pandas as pd
from tqdm import tqdm

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 MARCAP입니다. \n
        install()는 marcap 데이터 깃허브 주소를 알려주는 함수입니다. \n
        collect()는 데이터를 추출하는 함수입니다.
        """)

class MARCAP:

    def __init__(self, ticker_lst) -> list:
        """
        ticker_lst에 ticker를 넣어주세요.
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