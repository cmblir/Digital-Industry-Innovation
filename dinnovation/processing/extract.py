from sqlalchemy import create_engine
import pandas as pd

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is DataExtract. \n
        Enter database id, pw, port, database, table_name in order to connect.\n
        The connect() function is a function that tries to connect.\n
        The extract() function extracts the database after connecting.
        """)


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