import requests
from .constants import const
import pandas as pd
import json
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import certifi
from tqdm import tqdm
from datetime import datetime
import math
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is fmp_extact. \n
        get_jsonparsed_data() is a function that parses data. \n
        extractor() is a function that brings data in json format. \n
        url_generator() is a function that connects to the FMP site and separates data. \n
        ending_period_extact() is a function that normalizes dates. \n
        report_type_extract() is a function that determines whether it is annual or quarterly based on the incoming value. \n
        GetExcel() is a function that stores the extracted data. \n
        cleanse() is a function that processes data. \n
        get_symbols() is a function that gets data from the site. \n
        make_clean() is a function that sequentially executes the above functions to extract and save data.
        """)

class fmp_extract:
    def __init__(self):
        self.core_cols = const.fmp_core_cols 
        self.is_cols = const.fmp_is_cols 
        self.bs_cols = const.fmp_bs_cols
        self.cf_cols = const.fmp_cf_cols
        self.FMP_field = const.fmp_FMP_field


    def get_jsonparsed_data(self, url):
        """
        Receive the content of ``url``, parse it as JSON and return the object.

        Parameters
        ----------
        url : str

        Returns
        -------
        dict
        """
        response = urlopen(url, cafile=certifi.where())
        data = response.read().decode("utf-8")
        return json.loads(data)

    def extractor(self, url) : 
        try:
            from pandas import json_normalize
        except ImportError:
            from pandas.io.json import json_normalize
        req=requests.get(url)
        # json load
        data = json.loads(req.text)
        # json to pandas
        preprocessed = json_normalize(data)
        return preprocessed

    def url_generator(self, target_Symbol, filing_type, limit, api_key, period='quarter') : 
        base_url = f'https://financialmodelingprep.com/api/v3/income-statement/RY.TO?limit=5&period=quarter&apikey={api_key}'
        if period == 'annual' : 
            base_url = base_url.replace('&period=quarter','')
        
        is_url = base_url.replace('RY.TO', target_Symbol)
        is_url = is_url.replace('=5', '='+str(limit))
        bs_url = is_url.replace('income-statement','balance-sheet-statement')
        cf_url = is_url.replace('income-statement','cash-flow-statement')
        
        if filing_type == 'is' : 
            return is_url
        elif filing_type == 'bs' : 
            return bs_url
        elif filing_type == 'cf' : 
            return cf_url 
        else :
            return "Please input proper filing_type"

    def ending_period_extract(self, x) : 
        try : 
            date = datetime.strptime(str(x), '%Y-%m-%d')
            return date.strftime("%Y%m%d")
        except : 
            return 

    def report_type_extract(self, x) : 
        try : 
            if 'Q' in str(x) : 
                return 'Q'
            elif 'FY' in str(x) : 
                return 'A'
            else : 
                return 
        except : 
            return 

    def cleanse(self, path, filename, fund = False, trading = True) : 
        original_df = pd.read_excel(path)
        """
        data processing function
        original_df : Original data extracted from extract_and_save \n
        save_dir : Directory to save \n
        fund: True if you want to check ETFs listed on the stock market, otherwise False \n
        trading: True if you want to extract only companies that are currently trading in the stock market, otherwise False
        """

        if fund == True : 
            pass  # included fund
        else : 
            # false if you want to see only real listed company information with funds removed
            original_df['isFund'].fillna(False) 
            original_df = original_df[original_df['isFund']== False]
            
            
        # Extract only the companies that are currently trading
        if trading == True : 
            # Assume that firms with isActivelyTrading blank are trading
            original_df['isActivelyTrading'] = original_df['isActivelyTrading'].fillna(True)
            original_df = original_df[original_df['isActivelyTrading']==True]
        else : 
            pass
        
        
        ## Get correspondence table
        mapping_sheet = pd.DataFrame(self.FMP_field)
        # Dictionize correspondence table key : value = standard field name : FMP field name
        mapping_dic = mapping_sheet.set_index('채워야할 테이블 필드명').T.to_dict('index')['Financial Modeling API']
        
        processed = pd.DataFrame(columns = mapping_dic.keys())
        
        # Use the correspondence table with the loop below to insert information into an empty data frame called processed.
        for order, field in enumerate(processed.columns) : 
            try : 
                processed.loc[:,field] = original_df.loc[:,mapping_dic[field]]
            except : 
                pass
        import re 
        regExp = '\W[a-zA-Z]+'  # Remove special characters that are not letters and numbers with \W and '' with [a-zA-Z]+. Remove stock market code behind
        processed['lstng_cd']= processed['lstng_cd'].str.replace(pat = regExp ,repl=r'', regex = True)
        processed['stacnt_dt'] = processed['stacnt_dt'].apply(lambda x : self.ending_period_extract(x))
        processed['lstng_dt'] = processed['lstng_dt'].apply(lambda x : self.ending_period_extract(x))
        processed['fndtn_dt'] = processed['fndtn_dt'].apply(lambda x : self.ending_period_extract(x))
        processed['reprt_kind_cd'] = processed['reprt_kind_cd'].apply(lambda x : self.report_type_extract(x))
        
        processed.to_excel(filename, index = False)
        print(f"{filename}이 완료되었습니다.")
        
        return processed

    def get_symbols(self, country, api_key):
        """
        데이터 가져오는 함수 \n
        county에 국가명을 입력하세요. \n
        api_key is fmp api key
        """
        # Query String to get Symbols of companies with Financial Statements.
        url = (f"https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey={api_key}")
        Symbols = self.get_jsonparsed_data(url)

        US_symbols = []          # 
        Canada_symbols = []      #TO
        France_symbols = []      #PA
        Germany_symbols = []     #DE
        India_symbols = []       #NS
        London_symbols = []      #L
        Hongkong_symbols = []    #HK
        Australia_symbols = []  #AX
        Swiss_symbols = []       #SW
        Korea_symbols = []       #KR
        Netherlands_symbols = []

        for Symbol in Symbols : 
            if ".TO" in Symbol : 
                Canada_symbols.append(Symbol) 
            elif ".PA" in Symbol : 
                France_symbols.append(Symbol) 
            elif ".DE" in Symbol : 
                Germany_symbols.append(Symbol) 
            elif ".NS" in Symbol : 
                India_symbols.append(Symbol) 
            elif ".BS" in Symbol : 
                India_symbols.append(Symbol) 
            elif ".L" in Symbol : 
                London_symbols.append(Symbol) 
            elif ".HK" in Symbol : 
                Hongkong_symbols.append(Symbol) 
            elif ".AX" in Symbol : 
                Australia_symbols.append(Symbol) 
            elif ".SW" in Symbol : 
                Swiss_symbols.append(Symbol) 
            elif ".KS" in Symbol :
                Korea_symbols.append(Symbol)
            elif "." not in Symbol :
                US_symbols.append(Symbol) 
            elif ".EU" in Symbol : 
                Netherlands_symbols.append(Symbol)


        cnt = 0
        if country == "캐나다" : selected_symbols = Canada_symbols
        elif country == "프랑스" : selected_symbols = France_symbols
        elif country == "독일" : selected_symbols = Germany_symbols
        elif country == "인도" : selected_symbols = India_symbols
        elif country == "영국" : selected_symbols = London_symbols
        elif country == "홍콩" : selected_symbols = Hongkong_symbols
        elif country == "호주" : selected_symbols = Australia_symbols
        elif country == "스위스" : selected_symbols = Swiss_symbols
        elif country == "한국" : selected_symbols = Korea_symbols
        elif country == "미국" : selected_symbols = US_symbols
        elif country == "네덜란드" : selected_symbols = Netherlands_symbols
        selected_symbols = [i for i in selected_symbols if "\n" not in i]

        for _ in range(math.ceil(len(selected_symbols)/1000)):
            company_df_list = pd.DataFrame()
            for target_Symbol in tqdm(selected_symbols[cnt:cnt+1000]) :  
                if cnt == len(selected_symbols):
                    company_df_list = company_df_list.sort_values(by=['symbol','date'], ascending = [True,False]) # Sort symbols in ascending order, fiscal quarters within the same symbol in descending order.       
                    # As of 2022, only 5 years of data, that is, data from 2017 or more are extracted. -> calendarYear (fiscal year) is more than 2017
                    # company_df_list = company_df_list[company_df_list['calendarYear'].astype('int')>=2022]
                    company_df_list.to_excel(f'{country}_{cnt}.xlsx', index=False)

                for report_type in ['annual','quarter'] :
                
                    if report_type == 'annual' : 
                        limit = 2 # Annual financial statement 1 year
                    else : 
                        limit = 8 # Quarterly financial statements (4 quarters per year)
                
            
                    is_url = self.url_generator(target_Symbol, 'is', limit, api_key, report_type)
                    bs_url = self.url_generator(target_Symbol, 'bs', limit, api_key, report_type)
                    cf_url = self.url_generator(target_Symbol, 'cf', limit, api_key, report_type)

                    # Use the extractor function to import data through the API into a Pandas data frame.
                    df_is = self.extractor(is_url)
                    df_bs = self.extractor(bs_url)
                    df_cf = self.extractor(cf_url)


                # Error Avoidance
                # Occasionally, there are problems such as income statement and balance sheet, but no cash flow statement.
                # Error avoidance for all cases
                # try: If there is information about the columns to be extracted, call it
                # except: if there is information about the columns to be extracted, core_is()

                    try :  
                        df_is = df_is[self.core_cols+self.is_cols]
                    except : 
                        df_is = pd.DataFrame(columns=self.core_cols+self.is_cols)    
                    try :     
                        df_bs= df_bs[self.core_cols+self.bs_cols]
                    except : 
                        df_bs = pd.DataFrame(columns=self.core_cols+self.bs_cols)

                    try : 
                        df_cf = df_cf[self.core_cols+self.cf_cols]
                    except : 
                        df_cf = pd.DataFrame(columns=self.core_cols+self.cf_cols)


                    # merge above three dataframes 
                    #company_df = pd.concat([df_is,df_bs,df_cf],axis = 1)
                    company_df = pd.merge(df_is, df_bs, how = 'outer', on = self.core_cols)
                    company_df = pd.merge(company_df, df_cf, how = 'outer', on = self.core_cols)

                    # Add necessary company general information in addition to numeric data.
                    # Use Company profile API of Financil Modeling Prep.
                    base_url = f'https://financialmodelingprep.com/api/v3/profile/RY.TO?apikey={api_key}'
                    target_url = base_url.replace('RY.TO', target_Symbol)

                    # Get data with request
                    req = requests.get(target_url)
                    data = json.loads(req.text)

                    # Extract only necessary general information columns based on the data specification and add them to company_df.

                    try : company_df['companyName'] = data[0]['companyName'] 
                    except :company_df['companyName'] = ""
                    try : company_df['ceo'] = data[0]['ceo']
                    except :company_df['ceo'] = ""
                    try : company_df['phone'] =data[0]['phone']
                    except : company_df['phone'] = ""
                    try : company_df['website'] =data[0]['website']
                    except : company_df['website'] = ""
                    try : company_df['state'] =data[0]['state']
                    except : company_df['state'] = ""
                    try : company_df['city'] =data[0]['city']
                    except : company_df['city'] = ""
                    try : company_df['country'] =data[0]['country']
                    except : company_df['country'] = ""
                    try : company_df['industry'] =data[0]['industry']
                    except : company_df['industry'] = ""
                    try : company_df['ipoDate'] =data[0]['ipoDate']
                    except : company_df['ipoDate'] = ""
                    try : company_df['address'] =data[0]['address']
                    except : company_df['address'] = ""
                    try : company_df['zip'] =data[0]['zip']
                    except : company_df['zip'] = ""
                    try : company_df['exchangeShortName'] =data[0]['exchangeShortName']
                    except : company_df['exchangeShortName'] = ""
                    try : company_df['exchange'] =data[0]['exchange']
                    except :  company_df['exchange'] = ""
                    try : company_df['description'] =data[0]['description']
                    except : company_df['description'] = ""
                    try : company_df['isEtf'] =data[0]['isEtf']
                    except : company_df['isEtf'] = ""
                    try : company_df['isActivelyTrading'] =data[0]['isActivelyTrading']
                    except : company_df['isActivelyTrading'] = ""
                    try : company_df['isFund'] =data[0]['isFund']
                    except : company_df['isFund'] = ""
                        
                    # Add tax number. Use 'Company core information API'.
                    try : 
                        base_url = f'https://financialmodelingprep.com/api/v4/company-core-information?symbol=AAPL&apikey={api_key}'
                        target_url = base_url.replace('AAPL', target_Symbol)

                        # Get data with request
                        # If the imported data is null, an error will occur in the process of extracting information from 'data'.
                        req = requests.get(target_url)
                        data = json.loads(req.text)
                        
                        company_df['taxIdentificationNumber'] =data[0]['taxIdentificationNumber']
                        company_df['registrantName'] =data[0]['registrantName']

                    except : 
                        company_df['taxIdentificationNumber'] = ""
                        company_df['registrantName'] = ""
                    
                        
                        
                    company_df_list = company_df_list.append(company_df)
                    cnt += 1
                
                
            company_df_list = company_df_list.sort_values(by=['symbol','date'], ascending = [True,False]) # Sort symbols in ascending order, fiscal quarters within the same symbol in descending order.  

            # As of 2022, only 5 years of data, that is, data from 2017 or more are extracted. -> calendarYear (fiscal year) is more than 2017
            # company_df_list = company_df_list[company_df_list['calendarYear'].astype('int')>=2022]
            company_df_list.to_excel(f'Original_{country}_{cnt}.xlsx', index=False)

    def make_clean(self, FilePath, SavePath):
        """
        FilePath = directory to be cleaned \n
        SavePath = Directory to save after Clean operation
        """
        for File in tqdm(FilePath):
            name = File.split(".")[0]
            self.cleanse(f"{SavePath}/{File}", name)