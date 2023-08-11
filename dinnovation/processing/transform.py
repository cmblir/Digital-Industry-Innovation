import re
import pandas as pd
import random
import numpy as np
import warnings
import datetime
from tqdm import tqdm
from .constants import const
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('mode.chained_assignment',  None)

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The library includes T (Transform) in the ETL process. \n
        The class that checks data in the database is Checker. \n
        When designating a class, enter the id, pw, ip, db of the database (postgresql), and the table name to be extracted. \n
        The read_excel() function loads xlsx and saves it as a data frame. \n
        The read_csv() function loads a csv and saves it as a data frame. \n
        The data_update() function inputs I or U when updating new data. \n
        The date_update() function inputs the date when new data is updated. \n
        The CheckDate() function is a function that standardizes the general data date of Investing.com \n
        The CheckLength() function checks the size of data and cuts it by the size \n
        The CheckVarchar() function checks the financial data size and inserts a new one if it is large \n
        The CheckNumeric() function checks a number in financial data \n
        -------------------------------------------------- ----------------\n
        The class that checks data from database is Analysis. \n
        The read_excel() function loads xlsx and saves it as a data frame. \n
        The read_csv() function loads a csv and saves it as a data frame. \n
        The Fail() function is a function that dictates erroneous data to put into a data frame \n
        The CheckDate_Duplicate() function is a function that checks the date check and duplicate check \n
        The CheckNumber() function checks whether a phone number is valid \n
         """)

class Checker:
    """
    A module that inspects data and replaces values as needed
    """
    def __init__(self):
        self.definition = const.definition
        self.ColumnsDict = const.ColumnsDict
        self.Financial = const.Financial
        self.Month = const.Month
        self.NumericList = const.NumericList
        self.MonthDict = const.MonthDict
    
    def fndtn_dt(self):
        """
        A function that normalizes fndtn_dt values in data to YYYYMMDD
        """
        for idx in range(len(self.df)):
            value = self.df["fndtn_dt"][idx]
            if str(type(value)) == "<class 'numpy.int64'>": pass
            elif str(type(value)) == "<class 'datetime.datetime'>": self.df["fndtn_dt"][idx] = value.strftime("%Y%m%d")
            elif str(type(value)) == "<class 'numpy.float64'>":
                if value.astype(int) > 21000000 or value.astype(int) < 18000000:
                    if str(value) == "nan": self.df["fndtn_dt"][idx] = ""
            else:
                self.df["fndtn_dt"][idx] = ""

    def read_excel(self, path):
        """
        Enter the excel address in path.
        """
        self.df = pd.read_excel(path)

    def read_csv(self, path):
        """
        Enter the csv address in path.
        """
        self.df = pd.read_csv(path)

    def data_update(self, Insert: bool) -> bool:
        """
        Please enter whether or not to update new data.
        """
        if Insert == True:
            self.df["opert_sctin_cd"] = "I"
        elif Insert == False:
            self.df["opert_sctin_cd"] = "U"

    def date_update(self):
        """
        Function to input date update of new data
        """
        self.df["data_crtin_dt"] = "".join(str(datetime.date.today()).split("-"))

    def CheckDate(self):
        """
        Investing.com's function to standardize data dates \n
        General use only
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
        self.TableDefaultColumns = const.TableDefaultColumns
        self.TableDefaultColumns_info = const.TableDefaultColumns_info
        self.TableDefault = const.TableDefault
        self.DefaultTables = pd.DataFrame(self.TableDefault)
        self.DefaultColumns = pd.DataFrame(self.TableDefaultColumns)
        self.DefaultColumns_info = pd.DataFrame(self.TableDefaultColumns_info)
        self.ColumnDF = self.DefaultColumns.T
        self.ColumnDF = self.ColumnDF.rename(columns=self.ColumnDF.iloc[0])
        self.ColumnDF = self.ColumnDF.drop(self.ColumnDF.index[0])
        self.ColumnDF_info = self.DefaultColumns_info.T
        self.ColumnDF_info = self.ColumnDF_info.rename(columns=self.ColumnDF_info.iloc[0])
        self.ColumnDF_info = self.ColumnDF_info.drop(self.ColumnDF_info.index[0])
        self.ExceptList = ["stock_mrkt_cd", "acplc_lngg_stock_mrkt_nm", "engls_stock_mrkt_nm", "hb_ntn_cd", "crrnc_sctin_cd", 
                    "accnn_yr", "reprt_kind_cd","stacnt_dt", "opert_sctin_cd", "data_crtin_dt",
                    "cntct_prces_stts_cd", "cntct_prces_dt"]
        self.count = 0
        self.CheckDF = pd.DataFrame(columns=["번호", "시스템", "업무구분", "검증구분", "테이블명", "테이블ID", "컬럼명", "컬럼ID",
                                        "pk", "FK", "NN", "데이터타입", "도메인소분류",
                                        "도메인명", "전체건수", "오류건수", "오류율", "점검일시", "오류데이터"])
        self.DateList = list(self.DefaultColumns[self.DefaultColumns["데이터타입"] == "VARCHAR(8)"]["표준_영문컬럼명"].values)
        self.CheckDate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    def find_keys_by_value(dictionary, value):
        keys = []
        for k, v in dictionary.items():
            if v == value:
                keys.append(k)
        return keys
    
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

    def Fail(self, column, Failed, info):
        """
        Function to input error data
        column = column in error
        Failed = erroneous data
        """ 
        if info == True:
            ColumnName = self.ColumnDF_info[column].values[0] # 컬럼명
            ColumnDataType = self.ColumnDF_info[column].values[1] # 데이터타입
            FailedDict = {
                            "번호" : self.count,
                            "시스템" : "공통",
                            "업무구분" : "헤브론스타",
                            "검증구분" : self.verification,
                            "테이블명" : self.TableName,
                            "테이블ID" : self.TableID,
                            "컬럼명" : ColumnName,
                            "컬럼ID" : column,
                            "pk" : "N",
                            "FK" : "N",
                            "NN" : "N",
                            "데이터타입" : ColumnDataType,
                            "도메인소분류" : "",
                            "도메인명" : "",
                            "전체건수" : len(self.df),
                            "오류건수" : len(self.df[self.df[column] == Failed]),
                            "오류율" : round(len(self.df[self.df[column] == Failed]) / len(self.df), 3),
                            "점검일시" : self.CheckDate,
                            "오류데이터" : Failed
                        }
            return FailedDict

        else:
            ColumnName = self.ColumnDF[column].values[0] # 컬럼명
            ColumnDataType = self.ColumnDF[column].values[1] # 데이터타입
            FailedDict = {
                            "번호" : self.count,
                            "시스템" : "공통",
                            "업무구분" : "헤브론스타",
                            "검증구분" : self.verification,
                            "테이블명" : self.TableName,
                            "테이블ID" : self.TableID,
                            "컬럼명" : ColumnName,
                            "컬럼ID" : column,
                            "pk" : "N",
                            "FK" : "N",
                            "NN" : "N",
                            "데이터타입" : ColumnDataType,
                            "도메인소분류" : "",
                            "도메인명" : "",
                            "전체건수" : len(self.df),
                            "오류건수" : len(self.df[self.df[column] == Failed]),
                            "오류율" : round(len(self.df[self.df[column] == Failed]) / len(self.df), 3),
                            "점검일시" : self.CheckDate,
                            "오류데이터" : Failed
                        }
            return FailedDict

    def CheckDate_Duplicate(self, info=False):
        """
        A function that checks for duplicates at the same time as the date.
        """
        if info==True:
            self.df.columns = list(self.DefaultColumns_info["표준_영문컬럼명"].values)        
            for column in tqdm(self.df.columns): # 컬럼ID
                for length in range(len(self.df[column])):
                    self.verification = "날짜"
                    if column in self.DateList:
                        try: pd.to_datetime(str(self.df[column][length]), format='%Y%m%d')
                        except:
                            self.count += 1
                            Failed = self.df[column][length]
                            Returned = self.Fail(column, Failed, info)
                            self.CheckDF = self.CheckDF.append(Returned, ignore_index=True)
                if column in self.ExceptList: continue
                else:
                    for cwt, idx in zip(self.df[column].value_counts(), self.df[column].value_counts().index):
                        per = cwt / len(self.df)
                        self.verification = "중복"
                        if per >= 0.05:
                            self.count += 1
                            Failed = idx
                            Returned = self.Fail(column, Failed, info)
                            self.CheckDF = self.CheckDF.append(Returned, ignore_index=True)

        else:
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
        PN_1 = re.compile("\d{2,3}-\d{3,4}-\d{4}$") # phone number ex 1 000-0000-0000
        PN_2 = re.compile("\(\d{2,3}\)-\d{3,4}-\d{3,4}$") # phone number ex 2 (000)-0000-0000
        PN_3 = re.compile("\d{1,2} \d{7,8}$") # phone number ex 3 00 00000000
        BS_1 = re.compile("\d{1,2}-\d{7,8}$") # corporate number ex 1 00-00000000
        for column in phone_columns:
            for idx in range(len(self.df[column])):
                PN_Check_1 = PN_1.search(self.df[column][idx])
                PN_Check_2 = PN_2.search(self.df[column][idx])
                PN_Check_3 = PN_3.search(self.df[column][idx])
                if PN_Check_1 or PN_Check_2 or PN_Check_3:
                    pass
                else:
                    self.verification = "전화번호오류"
                    Failed = self.df[column][idx]
                    Returned = self.Fail(column, Failed)
                    self.CheckDF = self.CheckDF.append(Returned, ignore_index=True)
        for column in cop_columns:
            for idx in range(len(self.df[column])):
                BS_Check_1 = BS_1.search(self.df[column][idx])
                if BS_Check_1:
                    pass
                else:
                    self.verification = "법인번호오류"
                    Failed = self.df[column][idx]
                    Returned = self.Fail(column, Failed)
                    self.CheckDF = self.CheckDF.append(Returned, ignore_index=True)

    def change_columns(self, df, info=False, korea=False):
        """
        A function that converts collected data from standard Korean column names to English column names
        """
        if info==True: df.columns = [self.TableDefaultColumns_info[col] for col in df.columns]
        else: df.columns = [self.TableDefaultColumns["표준_영문컬럼명"][self.TableDefaultColumns["표준_한글컬렴명"].index(col)] if col in self.TableDefaultColumns["표준_한글컬렴명"] else col for col in df.columns]
        if korea==True: df.rename(columns={'법인등록번호': 'ovrss_entrp_crprt_rgno', '사업자등록번호': 'ovrss_entrp_bsnsm_rgno'}, inplace=True)
        return df