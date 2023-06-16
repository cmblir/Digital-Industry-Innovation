import re
import pandas as pd
from tqdm import tqdm
import random
import numpy as np
import warnings
import datetime
import T_constants as constants
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('mode.chained_assignment',  None)

class Checker:
    """
    Module that inspects data and replaces values as needed
    """
    def __init__(self):
        self.definition = constants.definition
        self.ColumnsDict = constants.ColumnsDict
        self.Financial = constants.Financial
        self.Month = constants.Month
        self.NumericList = constants.NumericList
        self.MonthDict = constants.MonthDict
        self.df = None

    def fndtn_dt(self):
        """
        Standardizes the fndtn_dt values in the data as YYYYMMDD
        """
        for idx in range(len(self.df)):
            value = self.df["fndtn_dt"][idx]
            if isinstance(value, np.int64):
                pass
            elif isinstance(value, datetime.datetime):
                self.df["fndtn_dt"][idx] = value.strftime("%Y%m%d")
            elif isinstance(value, np.float64):
                if value.astype(int) > 21000000 or value.astype(int) < 18000000:
                    if np.isnan(value):
                        self.df["fndtn_dt"][idx] = ""
            else:
                self.df["fndtn_dt"][idx] = ""


    def read_excel(self, path):
        """
        Please enter excel address in path.
        """
        self.df = pd.read_excel(path)

    def read_csv(self, path):
        """
        Enter the csv address in path.
        """
        self.df = pd.read_csv(path)

    def data_update(self, Insert: bool) -> bool:
        """
        Please enter whether to update new data.
        """
        if Insert == True:
            self.df["opert_sctin_cd"] = "I"
        elif Insert == False:
            self.df["opert_sctin_cd"] = "U"

    def date_update(self):
        """
        Function to enter date updates for new data
        """
        self.df["data_crtin_dt"] = "".join(str(datetime.date.today()).split("-"))

    def CheckDate(self):
        """
        Function \n to standardize the data date of the Investing Dotcom
        General only
        """
        for TimeLength in range(len(self.df)):
            time = self.df["entrp_reltn_tdngs_dt"][TimeLength]
            test = time.split()
            if len(time.split()) != 5:
                TimeStamp = test[5] + self.MonthDict[test[3].replace("(", "")] + test[4]
            else:
                TimeStamp = test[2] + self.MonthDict[test[0]] + test[1]
            self.df["entrp_reltn_tdngs_dt"][TimeLength] = re.sub(r'[^0-9]','',TimeStamp)
    
    def CheckLength(self):
        '''
        If it is larger than the size of the data, a function that cuts it to the size of the data \n
        Both financial and general
        '''
        for key, value in self.definition.items():
            for Length in range(len(self.df["keyval"])):
                check = str(self.df[key.lower()][Length])
                
                if len(check) > value:
                    self.df[key.lower()][Length] = str(self.df[key.lower()][Length])[:value]
                else:
                    pass

    def CheckVarchar(self, News=False): 
        """
        A function that checks the data size and inserts the data if it is larger than the data size \n
        finance only
        """
        FirstRange = len(self.df['keyval'])
        cnt = 0
        
        for Length in range(FirstRange, FirstRange + cnt):
            self.df[self.ColumnsDict["상세"].lower()][Length] = self.df[self.ColumnsDict["상세"].lower()][Length][4000:]
            self.df[self.ColumnsDict["출처"].lower()][Length] = self.df[self.ColumnsDict["출처"].lower()][Length][4000:]
            self.df[self.ColumnsDict["사내"].lower()][Length] = self.df[self.ColumnsDict["사내"].lower()][Length][4000:]
            self.df[self.ColumnsDict["고객"].lower()][Length] = self.df[self.ColumnsDict["고객"].lower()][Length][4000:]  
            self.df[self.ColumnsDict["공급"].lower()][Length] = self.df[self.ColumnsDict["공급"].lower()][Length][4000:]
            self.df[self.ColumnsDict["경쟁"].lower()][Length] = self.df[self.ColumnsDict["경쟁"].lower()][Length][4000:]   
            self.df[self.ColumnsDict["대체"].lower()][Length] = self.df[self.ColumnsDict["대체"].lower()][Length][4000:]

        if News == True:
            for finan in self.Financial:
                self.df[finan.lower()] = np.nan
                self.df["entrp_reltn_tdngs_kind_cont"] = "Stock News"
            for Length in range(FirstRange):
                if len(str(self.df[self.ColumnsDict["날짜"].lower()][Length])) > 8:
                    date = str(self.df[self.ColumnsDict["날짜"].lower()][Length]).split()[:3]
                    if date[0] not in self.Month:
                        if len(date) < 8:
                            rlst = ["20221206", "20221205", "20221204"]
                            date = rlst[random.randrange(0, 2)]
                            self.df[self.ColumnsDict["날짜"].lower()][Length] = date
                        else:
                            date = str(self.df[self.ColumnsDict["날짜"].lower()][Length]).split()[3:6]
                            monthly = str(self.Month.index(date[0].replace("(", "")) + 1).rjust(2, "0")
                            date = date[2] + monthly + date[1].replace(",", "")
                            self.df[self.ColumnsDict["날짜"].lower()][Length] = date
                    else:
                        monthly = str(self.Month.index(date[0]) + 1).rjust(2, "0")
                        date = date[2] + monthly + date[1].replace(",", "")
                        self.df[self.ColumnsDict["날짜"].lower()][Length] = date
            if len(str(self.df[self.ColumnsDict["상세"].lower()][Length])) > 4000:
                try:
                    self.df[self.ColumnsDict["상세"].lower()][Length] = self.df[self.ColumnsDict["상세"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["출처"].lower()][Length] = self.df[self.ColumnsDict["출처"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["사내"].lower()][Length] = self.df[self.ColumnsDict["사내"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["고객"].lower()][Length] = self.df[self.ColumnsDict["고객"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["URL"].lower()][Length] = self.df[self.ColumnsDict["URL"].lower()][Length][:1000]  
                    self.df[self.ColumnsDict["공급"].lower()][Length] = self.df[self.ColumnsDict["공급"].lower()][Length][:4000]
                    self.df[self.ColumnsDict["경쟁"].lower()][Length] = self.df[self.ColumnsDict["경쟁"].lower()][Length][:4000]   
                    self.df[self.ColumnsDict["대체"].lower()][Length] = self.df[self.ColumnsDict["대체"].lower()][Length][:4000]
                    self.df = self.df.append(self.df.iloc[Length:Length+1], ignore_index = True)
                    cnt += 1
                except:
                    pass
            else: pass


    def CheckNumeric(self):
        """
        A function that determines whether it is a number, deletes it if it is not a number, and derives only the number if it contains a number \n
        finance only
        """
        numeric_match = re.compile('[^0-9]')
        for i in range(len(self.df)):
            for column in self.NumericList:
                result_lst = []
                try:
                    split_lst = self.df[column.lower()][i].split()
                    for idx in split_lst:
                        if numeric_match.match(idx) == None:
                            result_lst.append(re.sub(r'[^0-9]', '', idx))
                    if len(result_lst) >= 1:
                        if column == "STACNT_DT":
                            if 18000000 < int(result_lst[0]) < 21000000:
                                self.df[column.lower()][i] = result_lst[0]
                            else:
                                self.df[column.lower()][i] = ""    
                        else:
                            self.df[column.lower()][i] = result_lst[0]
                    else:
                        pass
                except:
                    pass

class Analysis:
    def __init__(self):
        """
        A module that analyzes data and saves error rates in Excel format
        """
        self.TableDefaultColumns = constants.TableDefaultColumns
        self.TableDefault = constants.TableDefault
        self.DefaultTables = pd.DataFrame(self.TableDefault)
        self.DefaultColumns = pd.DataFrame(self.TableDefaultColumns)
        self.ColumnDF = self.DefaultColumns.T
        self.ColumnDF = self.ColumnDF.rename(columns=self.ColumnDF.iloc[0])
        self.ColumnDF = self.ColumnDF.drop(self.ColumnDF.index[0])
        self.ExceptList = ["stock_mrkt_cd", "acplc_lngg_stock_mrkt_nm", "engls_stock_mrkt_nm", "hb_ntn_cd", "crrnc_sctin_cd", 
                    "accnn_yr", "reprt_kind_cd","stacnt_dt", "opert_sctin_cd", "data_crtin_dt",
                    "cntct_prces_stts_cd", "cntct_prces_dt"]
        self.count = 0
        self.CheckDF = pd.DataFrame(columns=["번호", "시스템", "업무구분", "검증구분", "테이블명", "테이블ID", "컬럼명", "컬럼ID",
                                        "pk", "FK", "NN", "데이터타입", "도메인소분류",
                                        "도메인명", "전체건수", "오류건수", "오류율", "점검일시", "오류데이터"])
        self.DateList = list(self.DefaultColumns[self.DefaultColumns["데이터타입"] == "VARCHAR(8)"]["표준_영문컬럼명"].values)
        self.CheckDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def read_excel(self, path):
        """
        Enter the excel address in path.
        """
        self.df = pd.read_excel(path)
        if "/" in path: Table = path.split("/")[-1].split(".")[0]
        else: Table = path.split(".")[0]
        self.TableName = list(self.DefaultTables[self.DefaultTables["영문 테이블명"] == Table]["한국 테이블명"])[0]
        self.TableID = str(Table)

    def read_csv(self, path):
        """
        Enter the csv address in path.
        """
        self.df = pd.read_csv(path)
        if "/" in path: Table = path.split("/")[-1].split(".")[0]
        else: Table = path.split(".")[0]
        self.TableName = list(self.DefaultTables[self.DefaultTables["영문 테이블명"] == Table]["한국 테이블명"])[0]
        self.TableID = str(Table)

    def Fail(self, column, Failed):
        """
        Function to input error data
        column = column in error
        Failed = erroneous data
        """ 
        ColumnName = self.ColumnDF[column].values[0] # Column name
        ColumnDataType = self.ColumnDF[column].values[1] # Data Type
        FailedDict = {
                        "번호" : self.count,
                        "시스템" : "공통",
                        "업무구분" : "헤브론스타",
                        "검증구분" : self.verification,
                        "테이블명" : self.TableName,
                        "테이블ID" : self.TableID,
                        "컬럼명" : self.ColumnDF[column].values[0],
                        "컬럼ID" : column,
                        "pk" : "N",
                        "FK" : "N",
                        "NN" : "N",
                        "데이터타입" : self.ColumnDF[column].values[1],
                        "도메인소분류" : "",
                        "도메인명" : "",
                        "전체건수" : len(self.df),
                        "오류건수" : len(self.df[self.df[column] == Failed]),
                        "오류율" : round(len(self.df[self.df[column] == Failed]) / len(self.df), 3),
                        "점검일시" : self.CheckDate,
                        "오류데이터" : Failed
                    }
        return FailedDict

    def CheckDate_Duplicate(self):
        """
        A function that checks for duplicates at the same time as the date.
        """
        self.df.columns = list(self.DefaultColumns["표준_영문컬럼명"].values)        
        for column in tqdm(self.df.columns): # 컬럼ID
            for length in range(len(self.df[column])):
                self.verification = "날짜"
                if column in self.DateList:
                    try: pd.to_datetime(str(self.df[column][length]), format='%Y%m%d')
                    except:
                        self.count += 1
                        Failed = self.df[column][length]
                        Returned = self.Fail(column, Failed)
                        self.CheckDF = self.CheckDF.append(Returned, ignore_index=True)
            if column in self.ExceptList: continue
            else:
                for cwt, idx in zip(self.df[column].value_counts(), self.df[column].value_counts().index):
                    per = cwt / len(self.df)
                    self.verification = "중복"
                    if per >= 0.05:
                        self.count += 1
                        Failed = idx
                        Returned = self.Fail(column, Failed)
                        self.CheckDF = self.CheckDF.append(Returned, ignore_index=True)


    def CheckNumber(self, phone_columns, cop_columns):
        """
        Function to check columns of phone number
        """
        phone_number_regexes = [
            re.compile(r"\d{2,3}-\d{3,4}-\d{4}$"),  # ex 1 000-0000-0000
            re.compile(r"\(\d{2,3}\)-\d{3,4}-\d{3,4}$"),  # ex 2 (000)-0000-0000
            re.compile(r"\d{1,2} \d{7,8}$")  # ex 3 00 00000000
        ]
        business_number_regex = re.compile(r"\d{1,2}-\d{7,8}$")  # ex 1 00-00000000

        for column in phone_columns:
            for idx, value in enumerate(self.df[column]):
                if any(regex.search(value) for regex in phone_number_regexes):
                    continue
                else:
                    self.verification = "전화번호오류"
                    failed = value
                    returned = self.Fail(column, failed)
                    self.CheckDF = self.CheckDF.append(returned, ignore_index=True)
        
        for column in cop_columns:
            for idx, value in enumerate(self.df[column]):
                if business_number_regex.search(value):
                    continue
                else:
                    self.verification = "법인번호오류"
                    failed = value
                    returned = self.Fail(column, failed)
                    self.CheckDF = self.CheckDF.append(returned, ignore_index=True)


    def change_columns(self, df):
        """
        A function that converts collected data from standard Korean column names to English column names
        """
        df.columns = [self.TableDefaultColumns["표준_영문컬럼명"][self.TableDefaultColumns["표준_한글컬렴명"].index(col)] if col in self.TableDefaultColumns["표준_한글컬렴명"] else col for col in df.columns]
        return df