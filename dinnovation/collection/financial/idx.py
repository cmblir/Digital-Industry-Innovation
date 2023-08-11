import os
import pandas as pd
from tqdm import tqdm

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is idx_extact. \n
        make_Avaible() is a function that makes a data frame available. \n
        Add_On() is a function that creates data. \n
        transform() is a function that processes data.
        """)

class idx_extract:
    def __init__(self, dataframe):
        self.df = dataframe

    def MakeAvaible(self):
        try:
            self.df = self.df[["Unnamed: 3", "Unnamed: 1"]].T
            self.df = df.rename(columns=self.df.iloc[0])
            self.df = self.df.drop(index="Unnamed: 3")
            self.df = df.fillna(0)
        except:
            df = df[["Unnamed: 2", "Unnamed: 1"]].T
            df = df.rename(columns=df.iloc[0])
            df = df.drop(index="Unnamed: 2")
            df = df.fillna(0)
        return df

    def Add_On(self, mapping_path):
        self.mapping_sheet = pd.read_excel(mapping_path)
        self.mapping_dic = self.mapping_sheet.set_index('채워야할 테이블 필드명').T.to_dict('index')['idx']
        self.mapping_dic_2 = self.mapping_sheet.set_index('idx').T.to_dict('index')['채워야할 테이블 필드명']
        self.processed = pd.DataFrame(columns = self.mapping_dic.keys())
        self.Balance = self.mapping_sheet["Balance Sheet"]
        self.Income = self.mapping_sheet["Income Statement"]
        self.Cash = self.mapping_sheet["Cashflow"]
        self.General = self.mapping_sheet["General"]
        self.Balance = self.Balance.dropna()
        self.Income = self.Income.dropna()
        self.Cash = self.Cash.dropna()
        self.General = self.General.dropna()

    def transform(self, Path):
        FilePath = os.listdir(Path)
        for File in tqdm(FilePath):
            df_general = pd.read_excel(Path+File, sheet_name=1)
            df_balance = pd.read_excel(Path+File, sheet_name=2)
            df_income = pd.read_excel(Path+File, sheet_name=3)
            df_cash = pd.read_excel(Path+File, sheet_name=6)
            
            ticker_name = str(File).split("-")[3]
            quater = str(File).split("-")[2]
            success_columns = []
            success_index = []

            success_columns.append('주식시장코드')
            success_columns.append('현지언어주식시장명')
            success_columns.append('영문주식시장명')
            success_columns.append('헤브론스타국가코드')
            success_columns.append('통화구분코드')
            success_columns.append('회계연도')
            success_columns.append('보고서종류코드')
            success_columns.append('결산일자')


            success_index.append('IDX')
            success_index.append('IDX')
            success_index.append('IDX Stock')
            success_index.append('IDN')
            success_index.append('IDR')
            success_index.append(2022)
            success_index.append('Q')
            if quater == "I":
                success_index.append(20220228)
            elif quater == "II":
                success_index.append(20220530)
            elif quater == "III":
                success_index.append(20220830)



            for G_idx in self.General:
                try:
                    if self.MakeAvailble(df_general)[G_idx][0] == 0: pass
                    else:
                        success_columns.append(str(self.mapping_dic_2[G_idx]))
                        success_index.append(self.MakeAvailble(df_general)[G_idx][0])
                except: pass

            for B_idx in self.Balance:
                try:
                    if self.MakeAvailble(df_balance)[B_idx][0] == 0: pass
                    else:
                        success_columns.append(str(self.mapping_dic_2[B_idx]))
                        success_index.append(self.MakeAvailble(df_balance)[B_idx][0])
                except: pass
            
            for I_idx in self.Income:
                try:
                    if self.MakeAvailble(df_income)[I_idx][0] == 0: pass
                    else: 
                        success_columns.append(str(self.mapping_dic_2[I_idx]))
                        success_index.append(self.MakeAvailble(df_income)[I_idx][0])
                except: pass

            for C_idx in self.Cash:
                try: 
                    if self.MakeAvailble(df_cash)[C_idx][0] == 0: pass
                    else: 
                        success_columns.append(str(self.mapping_dic_2[C_idx]))
                        success_index.append(self.MakeAvailble(df_cash)[C_idx][0])
                except: pass

            if len(success_columns) > 0:     
                complete_df = pd.DataFrame([tuple(success_index)], columns=success_columns)
                processed = processed.append(complete_df, ignore_index=False)

        processed.to_excel("complete_indonesia_2022.xlsx")