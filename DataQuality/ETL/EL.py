import pandas as pd
import os
import psycopg2
import sqlalchemy
import constants
from sqlalchemy import create_engine
from tqdm import tqdm

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리는 ETL과정중 E(Extract)와 L(Load)을 포함하고 있습니다. \n
        데이터베이스에서 데이터를 추출하는 클래스는 DataExtract 입니다. \n
        클래스 지정시 데이터베이스(postgresql)의 id, pw, ip, db, 추출하고자 하는 테이블명을 입력해주세요. \n
        connect() 함수는 데이터베이스에 접속하는 함수입니다. \n
        extract() 함수는 데이터베이스에서 데이터를 추출하는 함수입니다. \n
        --------------------------------------------------- \n
        데이터베이스에 데이터를 적재하는 클래스는 DataLoad입니다. \n
        DataLoading() 함수는 데이터를 일괄로 불러오는 함수 \n
        CheckLength() 함수는 데이터내의 값의 최대 길이까지 제한을 두는 함수 \n
        Load() 함수는 데이터를 배치프로세스를 진행하여 적재하는 함수 \n
        Login() 함수는 데이터베이스에 로그인하는 함수 \n
        Connect_DB() 함수는 데이터베이스에 적재하기 위해 기본값을 설정하는 함수 \n
        """)


class DataExtract:
    """
    데이터베이스에서 데이터를 추출하는 모듈
    """
    def __init__(self, id, pw, ip, pt, db, table_name):
        """
        postgresql 아이디, 비밀번호, 접속ip, 데이터베이스명을 \n
        입력해주세요.
        """
        self.id = id
        self.pw = pw
        self.ip = ip
        self.pt = pt
        self.db = db
        self.table_name = table_name

    def connect(self):
        """
        데이터베이스에 접속하는 함수
        """
        self.url = f"postgresql://{self.id}:{self.pw}@{self.ip}:{self.pt}/{self.db}"
        self.engine = create_engine(self.url)
    
    def extract(self):
        """
        데이터베이스에서 데이터를 추출하는 함수
        """
        df = pd.read_sql_table(table_name = self.table_name, con=self.engine)
        return df.to_excel(f"{self.table_name}.xlsx")

class DataLoad:
    """
    데이터를 데이터베이스에 적재하는 모듈 
    사용할 데이터가 많을 시 many를 True로 설정해주세요.
    """
    def __init__(self, many=False):
        """
        다수의 파일을 적재할 시 many = True로 설정해주세요.
        """
        self.country = constants.country
        self.dtypesql_finan = constants.definition_finan
        self.definition_finan = constants.definition_finan
        self.dtypesql_info = constants.dtypesql_info
        self.definition_info = constants.definition_info
        self.empty_data = constants.empty_data
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
        데이터를 2개 이상 적재할 시 Path를 설정하여 사용하는 함수
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
        데이터의 크기보다 클 경우 해당 데이터 크기까지 자르는 함수
        한국 재무 안됨
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
        배치 프로세스를 이용한다. \n
        이는 일괄 처리라고도 하는 과정으로서 실시간으로 요청에 의해 처리되는 \n
        방식이 아닌 일괄적으로 대량의 데이터를 처리해준다.
        """
        engine = create_engine(self.url)
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
            return f"{self.table_name}가 적재 완료되었습니다."
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
                print(f"{self.table_nameList[length]}가 적재 완료되었습니다.")


    def Login(self, user, password, host, port, dbname):
        self.url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    def Connect_DB(self, replace=False, first=False):
        """
        postgresql 접속에 필요한 \n
        id, password, host, port, dbname이다.\n
        replace는 업데이트 여부를 의미한다. \n
        업데이트를 진행할 시 선택해주면 된다. \n
        각 국가마다 테이블명이 다르므로 필요하다.
        """
        self.replace = replace
        self.first = first
        if self.many == False:
            if first == False:
                try:
                    conn = psycopg2.connect(self.url)
                    cur = conn.cursor()
                    cur.execute(f"select keyval from {self.table_name} order by keyval desc limit 1;")
                    rows = cur.fetchall()
                    NowKeyval = int(str(rows[0]).split("'")[1])
                except psycopg2.DatabaseError as db_err:
                    print(f"현재 발생한 에러는 {db_err} 입니다.")
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
                        conn = psycopg2.connect(self.url)
                        cur = conn.cursor()
                        cur.execute(f"select keyval from {self.table_nameList[length]} order by keyval desc limit 1;")
                        rows = cur.fetchall()
                        NowKeyval = int(str(rows[0]).split("'")[1]) + 1
                    except psycopg2.DatabaseError as db_err:
                        print(f"현재 발생한 에러는 {db_err} 입니다.")
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