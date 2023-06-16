import pandas as pd
import EL_constants as constants
import psycopg2
from tqdm import tqdm
from sqlalchemy import create_engine
from pathlib import Path


class DataExtract:
    """
    Module for extracting data from a database
    """
    def __init__(self, id, pw, ip, pt, db, table_name):
        """
        Enter postgresql ID, password, access IP, and database name
        """
        self.id = id
        self.pw = pw
        self.ip = ip
        self.pt = pt
        self.db = db
        self.table_name = table_name
        self.url = f"postgresql://{self.id}:{self.pw}@{self.ip}:{self.pt}/{self.db}"
        self.engine = None

    def connect(self):
        """
        Function to connect to the database
        """
        try:
            self.engine = create_engine(self.url)
            # Connection check
            with self.engine.connect() as connection:
                print("Database connection successful")
        except Exception as e:
            print("Database connection failed: ", str(e))

    def extract(self):
        """
        Function to extract data from the database
        """
        try:
            df = pd.read_sql_table(table_name = self.table_name, con=self.engine)
            try:
                df.to_excel(f"{self.table_name}.xlsx")
                print(f"Data successfully extracted to {self.table_name}.xlsx")
            except Exception as e:
                print("Failed to save data to an Excel file: ", str(e))
        except Exception as e:
            print("Failed to extract data from the database: ", str(e))

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


    def data_loading(self, path):
        """
        If you're loading more than one dataset, specify the path.
        """
        def load_file(file_path):
            """
            Helper function to load a file based on its extension
            """
            if file_path.suffix == '.csv':
                try:
                    return pd.read_csv(file_path, encoding='cp949')
                except UnicodeDecodeError:
                    return pd.read_csv(file_path)
            elif file_path.suffix == '.xlsx':
                return pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")

        path = Path(path)
        for file_path in path.iterdir():
            table_name = file_path.stem
            df = load_file(file_path)

            if self.many:
                self.table_name_list.append(table_name)
                self.data_frame_list.append(df)
            else:
                self.table_name = table_name
                self.df = df
                break  # Assuming we only want to load the first file if self.many is False


    def CheckLength(self):
        """
        This function checks and limits the length of the data.
        Note: it doesn't work for Korean financial data.
        """
        # Wrapping single values in a list when not self.many
        table_list = self.table_nameList if self.many else [self.table_name]
        df_list = self.DataFrameList if self.many else [self.df]

        for table_name, df in zip(table_list, df_list):
            definition = self.definition_info if table_name.split("_")[-1] == "m" else self.definition_finan

            for key, value in tqdm(definition.items()):
                # Using pandas' vectorized operations and avoiding explicit loops
                df.loc[:, key.lower()] = df[key.lower()].apply(lambda x: str(x)[:value] if not pd.isna(x) and len(str(x)) > value else x)



    def Load(self):
        """
        This function utilizes batch processing, which is a process of handling 
        large amounts of data all at once rather than processing in real time.
        """
        engine = create_engine(self.url)
        
        # If not self.many, wrap self.table_name and self.df in lists
        table_list = self.table_nameList if self.many else [self.table_name]
        df_list = self.DataFrameList if self.many else [self.df]

        for table_name, df in zip(table_list, df_list):
            dtypesql = self.dtypesql_info if table_name.split("_")[-1] == "m" else self.dtypesql_finan
            if_exists = 'replace' if self.replace else 'append'
            
            df.to_sql(name=table_name, con=engine, schema='public', chunksize=10000,
                    if_exists=if_exists, index=False, dtype=dtypesql, method='multi')

            print(f"{table_name} has been loaded successfully.")



    def Connect_DB(self, replace=False, first=False):
        """
        Required for PostgreSQL connection: id, password, host, port, and dbname.
        Replace indicates whether an update is necessary.
        Select if you want to proceed with the update.
        A different table name is required for each country.
        """
        self.replace = replace
        self.first = first

        data_sources = [self.df] if not self.many else self.DataFrameList
        table_names = [self.table_name] if not self.many else self.table_nameList

        for df, table_name in zip(data_sources, table_names):
            if not first:
                try:
                    with psycopg2.connect(self.url) as conn:
                        with conn.cursor() as cur:
                            cur.execute(f"SELECT keyval FROM {table_name} ORDER BY keyval DESC LIMIT 1;")
                            rows = cur.fetchall()
                            NowKeyval = int(str(rows[0]).split("'")[1])
                except psycopg2.DatabaseError as db_err:
                    print(f"An error occurred: {db_err}")
                
                df["keyval"] = pd.Series(range(NowKeyval, len(df) + NowKeyval)).astype(int)
            else:
                df["keyval"] = pd.Series(range(len(df))).astype(int)

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
