import pandas as pd
import os
import psycopg2
from .constants import const
from sqlalchemy import create_engine
from tqdm import tqdm

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        The DataLoad() class is the main one. \n
        The class can handle large amounts if many = True is set. \n
        DataLoading() is a function that saves data in the form of a data frame within a class. \n
        CheckLength() is a function that measures the length of the saved data frame to prevent errors beyond the standard. In addition, the value of keyval is raised above the latest value that currently exists. \n
        Load() loads the data using a batch process. \n
        Login() is a function that connects to the database. \n
        Connect_DB() is a function that connects to the database and creates an environment where data can be loaded.
        """)

class DataLoad:
    """
    A module that loads data into the database
    Set many to True when there is a lot of data to use.
    """
    def __init__(self, many=False):
        """
        When loading multiple files, set many = True.
        """
        self.country = const.country
        self.dtypesql_finan = const.dtypesql_finan
        self.definition_finan = const.definition_finan
        self.dtypesql_info = const.dtypesql_info
        self.definition_info = const.definition_info
        self.empty_data = const.stock_name
        self.many = many
        self.url = None
        self.df = None
        self.table_name = None
        self.replace = False
        self.first = False
        self.table_nameList = []
        self.DataFrameList = []

    def DataLoading(self, Path):
        """
        A function used by setting a path when loading two or more data
        """
        FilePath = os.listdir(Path)
        if self.many == True:
            for file in tqdm(FilePath):
                tmp_table_name = file.split(".")[0]
                self.table_nameList.append(tmp_table_name)
                filetype = file.split(".")[1]
                if filetype == "csv":
                    try:
                        tmp = pd.read_csv(Path+file, encoding = "cp949")
                    except:
                        tmp = pd.read_csv(Path+file)
                elif filetype == "xlsx":
                    tmp = pd.read_excel(Path+file)       
                self.DataFrameList.append(tmp)
        else:
            for file in tqdm(FilePath):
                self.table_name = file.split(".")[0]
                filetype = file.split(".")[1]
                if filetype == "csv":
                    try:
                        self.df = pd.read_csv(Path+file, encoding = "cp949")
                    except:
                        self.df = pd.read_csv(Path+file)
                elif filetype == "xlsx":
                    self.df = pd.read_excel(Path+file)       

    def CheckLength(self):
        '''
        If it is larger than the data size, a function that cuts the data to that size
        Korea Finance No
        '''
        if self.many == False:
            if self.table_name.split("_")[-1] == "m":
                definition = self.definition_info
            elif self.table_name.split("_")[-1] == "d":
                definition = self.definition_finan 
            for key, value in tqdm(definition.items()):
                for Length in range(len(self.df)):
                    check = str(self.df[key.lower()][Length])
                    if check == "nan":
                        continue
                    elif len(check) > value:
                        self.df[key.lower()][Length] = str(self.df[key.lower()][Length])[:value]
                    else:
                        pass
        else:
            for length in tqdm(range(len(self.table_nameList))):
                if self.table_nameList[length].split("_")[-1] == "m":
                    definition = self.definition_info
                elif self.table_nameList[length].split("_")[-1] == "d":
                    definition = self.definition_finan 
                for key, value in tqdm(definition.items()):
                    for Length in range(len(self.DataFrameList[length])):
                        check = str(self.DataFrameList[length][key.lower()][Length])
                        if check == "nan":
                            continue
                        elif len(check) > value:
                            self.DataFrameList[length][key.lower()][Length] = str(self.DataFrameList[length][key.lower()][Length])[:value]
                        else:
                            pass


    def Load(self):
        """
        Use a batch process. \n
        This is a process, also known as batch processing, where \n is processed by request in real time.
        It processes large amounts of data in batches rather than in a way.
        """
        url = f"postgresql://{self.url['user']}:{self.url['password']}@{self.url['host']}:{self.url['port']}/{self.url['dbname']}"
        engine = create_engine(url)
        if self.many == False:
            if self.table_name.split("_")[-1] == "m":
                dtypesql = self.dtypesql_info
            elif self.table_name.split("_")[-1] == "d":
                dtypesql = self.dtypesql_finan
            if self.replace == False:
                self.df.to_sql(name = self.table_name, con=engine, schema='public',chunksize= 10000,
                if_exists='append', index = False, dtype=dtypesql, method = 'multi')
            elif self.replace == True:
                self.df.to_sql(name = self.table_name, con=engine, schema='public',chunksize= 10000,
                if_exists='replace', index = False, dtype=dtypesql, method = 'multi')
            return f"{self.table_name} has finished loading."
        else:
            for length in tqdm(range(len(self.table_nameList))):
                if self.table_nameList[length].split("_")[-1] == "m":
                    dtypesql = self.dtypesql_info
                elif self.table_nameList[length].split("_")[-1] == "d":
                    dtypesql = self.dtypesql_finan
                if self.replace == False:
                    self.DataFrameList[length].to_sql(name = self.table_nameList[length], con=engine, schema='public',chunksize= 10000,
                    if_exists='append', index = False, dtype=dtypesql, method = 'multi')
                elif self.replace == True:
                    self.DataFrameList[length].to_sql(name = self.table_nameList[length], con=engine, schema='public',chunksize= 10000,
                    if_exists='replace', index = False, dtype=dtypesql, method = 'multi')
                print(f"{self.table_nameList[length]} has finished loading.")


    def Login(self, user, password, host, port, dbname):
        self.url = {
                    'dbname': dbname,
                    'user': user,
                    'password': password,
                    'host': host,
                    'port': port
                }

    def Connect_DB(self, replace=False, first=False):
        """
        required to connect to postgresql \n
        id, password, host, port, dbname.\n
        replace means whether to update. \n
        You can choose to proceed with the update. \n
        It is necessary because the table name is different for each country.
        """
        self.replace = replace
        self.first = first
        if self.many == False:
            if first == False:
                try:
                    conn = psycopg2.connect(**self.url)
                    cur = conn.cursor()
                    cur.execute(f"select keyval from {self.table_name} order by keyval desc limit 1;")
                    rows = cur.fetchall()
                    NowKeyval = int(str(rows[0]).split("'")[1])
                except psycopg2.DatabaseError as db_err:
                    print(f"The current error is {db_err}")
                for keyval, idx in zip(range(NowKeyval, len(self.df)+NowKeyval), range(len(self.df))):
                    self.df["keyval"][idx] = int(keyval)
                self.df["keyval"] = self.df["keyval"].astype(int)
            else:
                for Length in tqdm(range(len(self.df))):
                    self.df["keyval"][Length] = int(Length)
                self.df["keyval"] = self.df["keyval"].astype(int)
        else:
            for length in tqdm(range(len(self.table_nameList))):
                if first == False:
                    try:
                        conn = psycopg2.connect(**self.url)
                        cur = conn.cursor()
                        cur.execute(f"select keyval from {self.table_nameList[length]} order by keyval desc limit 1;")
                        rows = cur.fetchall()
                        NowKeyval = int(str(rows[0]).split("'")[1]) + 1
                    except psycopg2.DatabaseError as db_err:
                        print(f"The current error is {db_err}")
                    for keyval, idx in zip(range(NowKeyval, len(self.DataFrameList[length])+NowKeyval), range(len(self.DataFrameList[length]))):
                        self.DataFrameList[length]["keyval"][idx] = int(keyval)
                    self.DataFrameList[length]["keyval"] = self.DataFrameList[length]["keyval"].astype(int)
                else:
                    for Length in tqdm(range(len(self.DataFrameList[length]))):
                        self.DataFrameList[length]["keyval"][Length] = int(Length)
                    self.DataFrameList[length]["keyval"] = self.DataFrameList[length]["keyval"].astype(int)
        
    def fill_data(self):
        for key, value in self.empty_data.items():
            for df_name, df in zip(self.table_nameList, self.DataFrameList):
                if key not in df_name:
                    continue
                df.update(df[['stock_mrkt_cd', 'acplc_lngg_stock_mrkt_nm', 'engls_stock_mrkt_nm']].fillna(value))
                df['hb_ntn_cd'] = df['hb_ntn_cd'].fillna(df_name.split('_')[2].upper())

    def change_name(self, df_name):
        df_name = next((f"tb_hb_{value}_plcfi_d.xlsx" for key, value in self.country.items() if key in df_name), df_name)
        return df_name