from sqlalchemy import create_engine
import pandas as pd

class DataExtract:
    def __init__(self, id, pw, ip, pt, db, table_name):
        """
        postgresql ID, password, connection ip, database name \n
        please enter
        """
        self.id = id
        self.pw = pw
        self.ip = ip
        self.pt = pt
        self.db = db
        self.table_name = table_name

    def connect(self):
        """
        Function to connect to database
        """
        self.url = f"postgresql://{self.id}:{self.pw}@{self.ip}:{self.pt}/{self.db}"
        self.engine = create_engine(self.url)
    
    def extract(self):
        """
        Function to extract data from database
        """
        df = pd.read_sql_table(table_name = self.table_name, con=self.engine)
        return df.to_excel(f"{self.table_name}.xlsx")