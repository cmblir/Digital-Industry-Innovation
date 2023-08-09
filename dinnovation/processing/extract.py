from sqlalchemy import create_engine
import pandas as pd

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