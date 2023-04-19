import pandas as pd
import numpy as np
import time
import datetime
import chromedriver_autoinstaller
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from webdriver_manager.chrome import ChromeDriverManager


class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        라이브러리는 2개로 나누어집니다. \n
        데이터를 수집하는 라이브러리인 Investing_Crawler, 데이터를 가공하는 라이브러리인 Investing_Cleanse \n
        ------------------------------------------------------------------------------------------\n
        Investing_Crawler의 함수는 아래와 같습니다. \n
        DriverSettings()은 셀레니움 크롬 드라이버 세팅 함수입니다. \n
        download_historial()은 과거 주종가 데이터를 수집하는 함수입니다. \n
        collect()은 인베스팅 닷컴에서 데이터를 수집하는 함수입니다. \n
        ------------------------------------------------------------------------------------------\n
        Investing_Cleanse는 클래스를 실행시키면 바로 진행이 됩니다. \n
        """)

class Investing_Crawler:

    def __init__(self, path):
        """
        path에 인베스팅 컬럼 엑셀을 넣어주세요.
        """
        self.base_url = 'https://au.investing.com'
        self.PROFILE_suffix = '-company-profile'
        self.IS_suffix = '-income-statement'
        self.BS_suffix = '-balance-sheet'
        self.CF_suffix = '-cash-flow'
        self.column_listup = pd.read_excel(path)
        # 인베스팅 닷컴의 계정과목 리스트들. 일반 기업, 은행, 보험업의 계정과목이 달랐다. 
        self.all_bs_cols = pd.concat([self.column_listup['BS'],self.column_listup['BANK BS'], self.column_listup['INSURANCE BS']]).dropna()
        self.all_is_cols = pd.concat([self.column_listup['IS'],self.column_listup['BANK IS'], self.column_listup['INSURANCE IS']]).dropna()
        self.all_cf_cols = pd.concat([self.column_listup['CF'],self.column_listup['BANK CF'], self.column_listup['INSURANCE CF']]).dropna()
        self.all_cols = pd.concat([self.column_listup['BS'],self.column_listup['BANK BS'], self.column_listup['INSURANCE BS'],self.column_listup['IS'],self.column_listup['BANK IS'], self.column_listup['INSURANCE IS'],
                            self.column_listup['CF'],self.column_listup['BANK CF'],self.column_listup['INSURANCE CF']]).dropna()

        # 일반기업, 은행, 보험업의 계정과목들중 겹치는 부분은 제거하여 중복없는 합집합을 사용한다. 
        # drop_duplicates는 중복되는 것들중 하나만 제거, 
        self.all_cols = self.all_cols.drop_duplicates()
        self.company_links = []

    def DriverSettings(self, Turn_off_warning = False, linux_mode = False) -> None:
        """
        드라이버 세팅을 하는 함수입니다.
        linux mode를 True로 지정할 경우 백그라운드에서 수집이 가능합니다.
        단, 클릭과 같은 액션은 취하지 못합니다.
        """
        if Turn_off_warning == True: self.TurnOffWarning()
        chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]  #크롬드라이버 버전 확인
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--incognito") # 시크릿 모드
        if linux_mode == True: chrome_options.add_argument("--headless") # 리눅스 GUI 없는 디스플레이 모드
        chrome_options.add_argument("--no-sandbox") # 리소스에 대한 엑서스 방지
        chrome_options.add_argument("--disable-setuid-sandbox") # 크롬 충돌 방지
        chrome_options.add_argument("--disable-dev-shm-usage") # 메모리 부족 에러 방지
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        try: # 크롬 드라이버
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)   
        except:
            chromedriver_autoinstaller.install(True)
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        # WebDruverException Error 방지 기존의 드라이버 버젼으로 지정
        # driver = webdriver.Chrome(executable_path='/Users/cmblir/Python/Musinsa-Analysis/100/chromedriver')

    def download_historial(self, all_atag_maintable, url):
        company_links = []
        for a in all_atag_maintable:
            company_link = a.attrs["href"] #find로 한번더 돌려준다.
            company_links.append(company_link)
        company_table_css = self.driver.find_element(By.CSS_SELECTOR, 'div[data-test="dynamic-table"]')
        company_table_html = company_table_css.get_attribute('outerHTML')
        company_table = pd.read_html(company_table_html)[0]
        stock_history_company_links = [i+"-historical-data" for i in company_links]
        for idx, name in tqdm(zip(stock_history_company_links[912:], company_table["Name"][912:])):
            self.driver.get("https://au.investing.com/" + idx)
            time.sleep(2)
            try: click_time = self.driver.find_element(By.XPATH, '//*[@id="history-timeframe-selector"]').click()
            except NoSuchElementException:
                self.driver.get("https://au.investing.com/" + idx)
                click_time = self.driver.find_element(By.XPATH, '//*[@id="history-timeframe-selector"]').click()
            click_month = self.driver.find_element(By.XPATH, '//*[@id="react-select-2-option-1"]').click()
            try: historical_data = self.driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/div[2]/main/div/div[6]/div/div/div[2]/div[2]')
            except NoSuchElementException: 
                try:
                    historical_data = self.driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/div[2]/main/div/div[7]/div/div/div[2]/div[2]')
                except: 
                    self.driver.refresh()
                    click_time = self.driver.find_element(By.XPATH, '//*[@id="history-timeframe-selector"]').click()
                    click_month = self.driver.find_element(By.XPATH, '//*[@id="react-select-2-option-1"]').click()
                    try: historical_data = self.driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/div[2]/main/div/div[6]/div/div/div[2]/div[2]')
                    except NoSuchElementException: historical_data = self.driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/div[2]/main/div/div[7]/div/div/div[2]/div[2]')
            time.sleep(2)
            historical_data.click()
            input_element = historical_data.find_element(By.TAG_NAME, 'input')
            while input_element.get_attribute('value') != "2018-01-01":
                input_element.clear()
                input_element.send_keys("20180101")
            month_button = historical_data.find_element(By.TAG_NAME, 'button')
            month_button.send_keys("\n")
            time.sleep(3)
            a = historical_data.find_element(By.TAG_NAME, 'a')
            a.click()
        self.driver.get(url)

    def collect(self, country, official_countryName ,save_dir, isSingapore=False) : 
        # 멕시코 주식시장 정보 페이지 접속
        first_url = f'https://au.investing.com/equities/{country.lower()}'    #맨 뒤에 'mexico'를 바꾸면 다른 국가에서도 재활용 할 수 있음
        self.driver.get(first_url)
        time.sleep(8)
        breakpoint()
        #페이지에 표시되는 주식을 'all stocks'로 바꿈 
        try : 
            if isSingapore == False : 
                select_box = self.driver.find_element(By.XPATH,'//*[@id="index-select"]/div[1]').click()
                select_all_stock = self.driver.find_element(By.XPATH,'//*[@id="index-select"]/div[2]/div/div/div[1]').click()
            else : 
                pass
            time.sleep(5)

        # Avoid error. 만약 광고 팝업창으로 인해 에러가 발생할 경우 except로 에러 회피
        except : 
            # 광고 팝업창 닫기 
            self.driver.find_element(By.XPATH, '//*[@id="PromoteSignUpPopUp"]/div[2]/i').click()
            # try와 동일

            # 싱가포르는 all stock을 선택하는 탭이 없다. 따라서 all stocks 버튼을 클릭하는 부분을 
            # 건너 뛰고 넘어간다. 
            if isSingapore == False : 
                select_box = self.driver.find_element(By.XPATH,'//*[@id="stocksFilter"]').click()
                select_all_stock = self.driver.find_element(By.XPATH,'//*[@id="all"]').click()
            else : 
                pass
            time.sleep(5)

        # 인베스팅 닷컴은 동적페이지이다. 
        # BeautifulSoup을 사용할수 있도록 스크롤을 한번 끝까지 내렸다 올린다.
        SCROLL_PAUSE_SEC = 3

        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            
            try:
                some_tag = self.driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/div[2]/main/div[3]/div[2]/div')
                action = ActionChains(self.driver)
                action.move_to_element(some_tag).perform()
                some_tag.click()
            except NoSuchElementException: break
            # 끝까지 스크롤 다운
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # SCROLL_PAUSE_SEC만큼 대기
            time.sleep(SCROLL_PAUSE_SEC)
            
            # 스크롤 다운 후 스크롤 높이 다시 가져옴
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        time.sleep(20)
        soup = BeautifulSoup(self.driver.page_source,"html.parser")

        # 주식 테이블을 가지고 온다. 
        maintable = soup.find('div', {'data-test': 'dynamic-table'})

        # 메인 주식 테이블에서 회사 페이지로
        all_atag_maintable = maintable.find_all('a')

        # soup의 find 메서드를 이용해서 갖고 오게 되면, 링크 뿐 아니라 다른 정보들도 같이 갖고 오게 됩니다. 
        # 따라서 아래 for문으로 순수한 href 링크만을 추출하여, company_links에 저장합니다. 
        for a in all_atag_maintable:
            company_link = a.attrs["href"] #find로 한번더 돌려준다.
            self.company_links.append(company_link)

        # Version 3

        self.download_historial(all_atag_maintable, first_url)

        wait_time = 3 # time.sleep의 변수 


        # 일반기업, 은행, 보험업의 계정과목들중 겹치는 부분은 제거하여 중복없는 합집합을 사용한다. 
        # drop_duplicates는 중복되는 것들중 하나만 제거, 그리고 리스트화 
        all_cols = list(self.all_cols.drop_duplicates())
        all_cols = all_cols+['is_unit','bs_unit','cf_unit','is_time','bs_time','cf_time','report_type','company_name','industry_info','sector_info',
                            'address_info','phone_info','fax_info','webpage_info','source','gathering_time','PIC']

        # all_cols에 기업 이름, 일반정보 등을 추가한다. 

        self.result_df = pd.DataFrame()
        crawling_failed_companies = []
        for company in self.company_links : 

            profile_url = self.base_url+company+self.PROFILE_suffix
            bs_url = self.base_url+company+self.BS_suffix
            is_url = self.base_url+company+self.IS_suffix
            cf_url = self.base_url+company+self.CF_suffix

            try : 
                # company_link를 보면 'cid='라 주소 맨 끝에 붙어져 있는, 링크가 특이한 기업들이 있다. 이런 기업들은 다루 처리를 해주어야 한다. 
                if company.__contains__('cid=') : 
                    index = company.find('?')
                    profile_url =  self.base_url+company[:index]+self.PROFILE_suffix+company[index:]
                    bs_url = self.base_url+company[:index] +self.BS_suffix+company[index:]
                    is_url = self.base_url+company[:index] +self.IS_suffix+company[index:]
                    cf_url = self.base_url+company[:index] +self.CF_suffix+company[index:]

                ############## 기업 일반정보   #########
                #company profile page
                self.driver.get(profile_url)
                time.sleep(1)

                #  description : 기업 설명
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                desciption_info = soup.find('div', attrs = {'class' : 'companyProfileBody'}).text
                desciption_info = desciption_info.replace('\n','')

                profile_header = soup.find('div', attrs = {'class' : 'companyProfileHeader'}).text

                # Industry, Sector, Equity Type        
                industry_info =  profile_header.split('\n')[1].replace('Industry','')
                sector_info = profile_header.split('\n')[2].replace('Sector','')

                # contact info 
                address_info = soup.find('div', attrs = {'class' : 'companyAddress'}).text
                phone_info = soup.find('div', attrs = {'class' : 'companyPhone'}).text
                fax_info = soup.find('div', attrs = {'class' : 'companyFax'}).text
                webpage_info = soup.find('div', attrs = {'class' : 'companyWeb'}).text

                # 특수문자 제거 
                phone_info = phone_info.replace('\n','')
                phone_info = phone_info.replace('Phone','')
                fax_info = fax_info.replace('\n','')
                fax_info = fax_info.replace('Fax','')
                webpage_info = webpage_info.replace('\n','')
                webpage_info = webpage_info.replace('Web','')

                ############## 기업 재무정보   #########    
                # 분기별 Income Statement 
                self.driver.get(is_url)
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break

                df_income_Q = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_is_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text             # 통화 단위(Unit)
                company_name = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[1]/h1').text             # 회사이름 추출

                # income statement dataframe preprocess
                df_income_Q = df_income_Q.T
                df_income_Q = df_income_Q.rename(columns = df_income_Q.iloc[0])         # 계정과목을 column 이름으로 설정
                df_income_Q = df_income_Q.iloc[1:, :]                                 # 0번째 row를 삭제하고 나머지만 남긴다.

                df_income_Q['is_unit'] = df_is_unit                 # 데이터프레임에 income statement 통화단위 삽입
                df_income_Q['is_time']= df_income_Q.index                  # 데이터프레임에 '시간' 추가
                df_income_Q['report_type'] = 'Quarter'                         # 데이터프레임에 분기보고서임을 표시 
                df_income_Q['company_name'] = company_name                     # 데이터프레임에 회사 이름 삽입
                df_income_Q=df_income_Q.reset_index(drop=True) 

                # 연간 Income Statement. 
                try : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[8]/div[1]/a[1]').click() # Annual 버튼 클릭
                    time.sleep(2)
                except : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[9]/div[1]/a[1]').click()
                    time.sleep(2)    

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break

                df_income_A = pd.read_html(self.driver.page_source)[table_num].dropna()

                # income statement dataframe preprocess
                df_income_A = df_income_A.T
                df_income_A = df_income_A.rename(columns = df_income_A.iloc[0])         # 계정과목을 column 이름으로 설정
                df_income_A = df_income_A.iloc[1:, :]                                 # 0번째 row를 삭제하고 나머지만 남긴다.

                df_income_A['is_unit'] = df_is_unit                 # 데이터프레임에 income statement 통화단위 삽입
                df_income_A['is_time']= df_income_A.index                  # 데이터프레임에 '시간' 추가
                df_income_A['report_type'] = 'Annual'                          # 데이터프레임에 분기보고서임을 표시 
                df_income_A['company_name'] = company_name                     # 데이터 프레임에 회사 이름 삽입
                df_income_A=df_income_A.reset_index(drop=True) 


                # balance data
                self.driver.get(bs_url)
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order


                # 분기별 balance sheet
                df_balance_Q = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_bs_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text           # 통화 단위(Unit) 추출
                time.sleep(wait_time)
                # balance sheet dataframe preprocess
                df_balance_Q = df_balance_Q.T
                df_balance_Q = df_balance_Q.rename(columns = df_balance_Q.iloc[0])     # 계정과목을 column 이름으로 설정
                df_balance_Q = df_balance_Q.iloc[1:, :]                               # 0번째 row를 삭제하고 나머지만 남긴다.
                ##df_balance = df_balance[bs_targets]
                df_balance_Q['bs_unit'] = df_bs_unit                  # 데이터프레임에 cash flow 통화단위 삽입
                df_balance_Q['bs_time']= df_balance_Q.index
                #df_balance_Q['report_type'] = 'Quarter'                         # 데이터프레임에 분기보고서임을 표시 
                #df_balance_Q['company_name'] = company_name                     # 데이터 프레임에 회사 이름 삽입
                df_balance_Q=df_balance_Q.reset_index(drop=True)


                # 연간 balance sheet 
                try : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[8]/div[1]/a[1]').click() # Annual 버튼 클릭
                except :
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[9]/div[1]/a[1]').click() # Annual 버튼 클릭
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break

                df_balance_A = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_bs_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text           # 통화 단위(Unit) 추출
                time.sleep(wait_time)
                # balance sheet dataframe preprocess
                df_balance_A = df_balance_A.T
                df_balance_A = df_balance_A.rename(columns = df_balance_A.iloc[0])      # 계정과목을 column 이름으로 설정
                df_balance_A = df_balance_A.iloc[1:, :]                               # 0번째 row를 삭제하고 나머지만 남긴다.
                ##df_balance = df_balance[bs_targets]
                df_balance_A['bs_unit'] = df_bs_unit                  # 데이터프레임에 cash flow 통화단위 삽입
                df_balance_A['bs_time']= df_balance_A.index
                #df_balance_A['report_type'] = 'Annual'                          # 데이터프레임에 연간보고서임을 표시 
                #df_balance_A['company_name'] = company_name                     # 데이터 프레임에 회사 이름 삽입
                df_balance_A = df_balance_A.reset_index(drop=True)

                # cash flow
                self.driver.get(cf_url)
                time.sleep(2)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break    

                # 분기별 cashflow 
                df_cash_flow_Q = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_cf_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text           # 통화 단위(Unit) 추출
                time.sleep(wait_time)

                df_cash_flow_Q = df_cash_flow_Q.T 
                df_cash_flow_Q = df_cash_flow_Q.rename(columns = df_cash_flow_Q.iloc[0]) # 계정과목을 column 이름으로 설정
                df_cash_flow_Q = df_cash_flow_Q.iloc[1:, :]                              # 0번째 row를 삭제하고 나머지만 남긴다.
                df_cash_flow_Q['cf_unit'] = df_cf_unit                     # 데이터프레임에 cash flow 통화단위 삽입
                df_cash_flow_Q['cf_time']= df_cash_flow_Q.index.get_level_values(0)  # cf는 period_ending, period_length 두가지 멀티인덱스 사용    
                df_cash_flow_Q = df_cash_flow_Q.reset_index(drop=True)

                #연도별 cashflow 
                try : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[8]/div[1]/a[1]').click() # Annual 버튼 클릭
                except :                         
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[9]/div[1]/a[1]').click() # Annual 버튼 클릭
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break        

                df_cash_flow_A = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_cash_flow_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text            # 통화 단위(Unit) 추출

                #cash flow dataframe preprocess
                df_cash_flow_A = df_cash_flow_A.T 
                df_cash_flow_A = df_cash_flow_A.rename(columns = df_cash_flow_A.iloc[0]) # 계정과목을 column 이름으로 설정
                df_cash_flow_A = df_cash_flow_A.iloc[1:, :]                              # 0번째 row를 삭제하고 나머지만 남긴다.
                df_cash_flow_A['cf_unit'] = df_cf_unit                     # 데이터프레임에 cash flow 통화단위 삽입
                df_cash_flow_A['cf_time']= df_cash_flow_A.index.get_level_values(0)  # cf는 period_ending, period_length 두가지 멀티인덱스 사용 
                df_cash_flow_A = df_cash_flow_A.reset_index(drop=True) 

                # 일괄적으로 통합 열방향으로.
                # company_df_A는 'Annual'버튼을 눌러 얻은 사업보고서 
                company_df_A = pd.concat([df_balance_A, df_income_A, df_cash_flow_A],axis=1) # 데이터프레임에 사업보고서임을 표시 
                company_df_A['industry_info'] = industry_info
                company_df_A['sector_info'] = sector_info
                company_df_A['address_info'] = address_info
                company_df_A['phone_info'] = phone_info
                company_df_A['fax_info'] = fax_info
                company_df_A['webpage_info'] = webpage_info
                company_df_A['source'] = 'https://au.investing.com'+company+self.IS_suffix
                company_df_A['PIC'] = 'Nicholas'
                company_df_A['gathering_time'] = datetime.date.today()
                
                # company_df_Q는 'Quarter'버튼을 눌러 얻은 분기보고서
                company_df_Q = pd.concat([df_balance_Q, df_income_Q, df_cash_flow_Q],axis=1)# 데이터프레임에 분기보고서임을 표시 
                company_df_Q['industry_info'] = industry_info
                company_df_Q['sector_info'] = sector_info
                company_df_Q['address_info'] = address_info
                company_df_Q['phone_info'] = phone_info
                company_df_Q['fax_info'] = fax_info
                company_df_Q['webpage_info'] = webpage_info
                company_df_Q['source'] = 'https://au.investing.com'+company+self.IS_suffix
                company_df_Q['PIC'] = 'Nicholas'
                company_df_Q['gathering_time'] = datetime.date.today()

                
                blank = pd.DataFrame(columns = all_cols)
                for code in blank.columns : 
                # 대응표의 ifrs코드로 필요한 정보를 추출한다. 
                    try : 
                        blank[code] = company_df_A[code] 
                    except : 
                        pass  

                self.result_df = self.result_df.append(blank)
                blank = pd.DataFrame(columns = all_cols)

                for code in blank.columns : 
                # 대응표의 ifrs코드로 필요한 정보를 추출한다. 
                    try : 
                        blank[code] = company_df_Q[code] 
                    except : 
                        pass  


                self.result_df = self.result_df.append(blank)

            except :
                # 만약 재무정보가 없어 에러가 났을 경우, 일반정보라도 추가한다. 
                ############## 기업 일반정보   #########
                #company profile page
                self.driver.get(profile_url)
                time.sleep(1)

                #  description : 기업 설명
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                try : 
                    company_name = soup.find('h2').text.replace('Company Profile',"")
                    #  description : 기업 설명
                    desciption_info = soup.find('div', attrs = {'class' : 'companyProfileBody'}).text
                    desciption_info = desciption_info.replace('\n','')
                except : 
                    print(f"profile이 없어서 안되는 기업 {profile_url}")
                    print(f"profile이 없어서 안되는 기업 {company}")
                    continue

                profile_header = soup.find('div', attrs = {'class' : 'companyProfileHeader'}).text

                # Industry, Sector, Equity Type        
                industry_info =  profile_header.split('\n')[1].replace('Industry','')
                sector_info = profile_header.split('\n')[2].replace('Sector','')

                # contact info 
                address_info = soup.find('div', attrs = {'class' : 'companyAddress'}).text
                phone_info = soup.find('div', attrs = {'class' : 'companyPhone'}).text
                fax_info = soup.find('div', attrs = {'class' : 'companyFax'}).text
                webpage_info = soup.find('div', attrs = {'class' : 'companyWeb'}).text


                # 정규표현식으로 특수문자 제거 
                phone_info = phone_info.replace('\n','')
                phone_info = phone_info.replace('Phone','')
                fax_info = fax_info.replace('\n','')
                fax_info = fax_info.replace('Fax','')
                webpage_info = webpage_info.replace('\n','')
                webpage_info = webpage_info.replace('Web','')


                blank = pd.DataFrame(columns = all_cols)
                blank['company_name'] = company_name
                blank['industry_info'] = industry_info
                blank['sector_info'] = sector_info
                blank['address_info'] = address_info
                blank['phone_info'] = phone_info
                blank['fax_info'] = fax_info
                blank['webpage_info'] = webpage_info
                blank['source'] = 'https://au.investing.com'+company+self.PROFILE_suffix
                blank['PIC'] = 'Nicholas'
                blank['gathering_time'] = datetime.date.today()


                for code in blank.columns : 
                    # 대응표의 ifrs코드로 필요한 정보를 추출한다. 
                    try : 
                        blank[code] = company_df_A[code] 
                    except : 
                        pass  
                
                self.result_df = self.result_df.append(blank)
                
        self.result_df['Country'] = official_countryName
        self.result_df.to_excel(save_dir ,index = False)
        self.driver.quit()
        
        return self.result_df 

class Investing_Cleanse: 
        
    def __init__(self, originalfile_dir, mapping_sheet_dir, 통화코드결측대체 = True):
        self.data = pd.read_excel(originalfile_dir)
        # 회사 이름이 없는 row 제거
        self.data = self.data.dropna(subset = ['company_name'])
        # 통화구분코드에 대응하는 'currency'라는 열을 data 에 추가 
        self.data['currency'] = self.data['bs_unit'].apply(lambda x : self.currency_extract(x))
        # 'unit' 열을 self.data 에 추가 
        self.data['unit'] = self.data['unit'] = self.data['bs_unit'].apply(lambda x : self.unit_change(x)) 
        # 원본데이터에서 숫자데이터가 들어가는 열은 0번째 열부터 141번째 열까지임
        # self.data['unit']으로 숫자 데이터들의 단위를 바꿔주기 이전에, 가끔 null값이 '-'으로 들어가 있어서 이것을 먼저 제거토록 한다. 
        for name in self.data.columns[:142] : self.data[name]= self.data[name].replace('-',np.nan)
        # 숫자 데이터들은 단위를 바꿔준다. 
        # 0열부터 113열까지가 숫자 데이터가 있는 열들 
        for name in self.data.columns[:142] : self.data[name]= self.data[name].astype('float')*self.data['unit']
        # 기업이름에서 티커를 추출하여 'stock_code'라는 열을 추가한다. 
        self.data['stock_code'] = self.data['company_name'].apply(lambda x : self.ticker_extract(x))
        # 기업이름에서 티커를 추출했으므로 티커를 제거한다. 
        self.data['company_name'] = self.data['company_name'].apply(lambda x : self.ticker_delete(x))
        # 회계연도와 회계분기를 생성하기 위해 bs_time을 date_time 형식으로 가공하여 
        # 'ending' period라는 열을 추가한다. 
        self.data['ending_period'] = self.data['bs_time'].apply(lambda x : self.ending_period_extract(x))
        # 결측치 제거 
        # 회계연도와 회계 분기를 뽑아내기전, ending_perod가 없는 결측치를 먼저 제거한다. 
        self.data = self.data.dropna(subset=['ending_period'])
        # 헤브론 스타 국가코드 열 채우기 
        self.data['hb_nation_code'] = self.data['Country'].apply(lambda x : self.hb_nation_code(x))
        # 통화코드가 없는 데이터들은 최빈값 (frequent value)로 대체하거나 제거한다. 
        if 통화코드결측대체 == True : self.data['currency'] = self.data['currency'].fillna(value = self.data['currency'].mode()[0])
            # 해당 국가에서 가장 빈번하게 쓰이는 통화코드로 통화코드가 없는 칸들을 채워넣는다. 
        else : self.data = self.data.dropna(subset=['currency'])
            # 만약통화코드결측대체가 False이면 결측값이 있는 row는 제거한다. 
        # ending_period를 이용하여 회계연도에 대응하는 Fiscal year 컬럼 생성
        self.data['Fiscal year'] = self.data['ending_period'].apply(lambda x : x.year)
        # ending_period를 이용하여 결산일자에 대응하는 ending_period 컬럼 생성 
        self.data['ending_period'] = self.data['ending_period'].apply(lambda x : x.strftime("%Y%m%d"))
        # 비유동자산 추출을 위한 가공
        self.data['Total Assets - Total Current Assets'] = self.data['Total Assets']-self.data['Total Current Assets']
        # 부채자산총계 추출을 위한 가공 
        self.data['Total Liabilities + Total Equity'] = self.data['Total Liabilities']+self.data['Total Equity']
        # 보고서종류 
        self.data['report_type'] = self.data['report_type'].apply(lambda x : self.report_type_generator(x))    
        self.mapping_sheet = pd.read_excel(mapping_sheet_dir)
        self.mapping_dic = self.mapping_sheet.set_index('2022 field name').T.to_dict('index')['corresponding field name 1']
        self.mapping_dic_alternative = self.mapping_sheet.set_index('2022 field name').T.to_dict('index')['corresponding field name 2']
        # procssed라는 빈 데이터 프레임을 만든 후 아래 루프문으로 'InvestingDotcom 수집코드'로 크롤링한 원본데이터(df)를 
        # 표준화 한다. 
        self.processed = pd.DataFrame(columns = self.mapping_dic.keys())

    # bs_unit에서 통화구분코드를 추출하는 함수     
    def currency_extract(self, x) :
        world_currency = ['HKD', 'EUR', 'AUD', 'KRW', 'NZD', 'PHP', 'USD', 'DKK', 'TRY', 'CAD', 'CLP','INR', 'EGP', 'NOK', 'MYR', 'MXN', 
                        'CHF', 'GBP', 'SGD', 'ARS', 'THB', 'JPY', 'CNY','IDR','VND']
        for currency in world_currency : 
            try : 
                if str(currency) in x : return currency 
                else : continue
            except : return np.nan
        return np.nan    
    
    # 재무제표에 사용된 숫자 단위를 추출하는 함수  
    def unit_change(self,x) : 
        try : 
            if "Millions" in x : unit =  1000000
            elif "Billions" in x : unit = 1000000000
            else : unit = 0
        except : unit = x
        return unit
    
    # 기업 이름에서 티커를 추출하는 함수 
    def ticker_extract(self,x) : 
        ticker = re.findall('\(([^)]+)',x)
        ticker = ticker[0] # findall은 list를 return 하므로 인덱싱을 활용하여 원소만 추출
        return ticker
    
    # 기업 이름에서 티커를 제거하는 함수 
    def ticker_delete(self,x) : 
        regex = "\(.*\)|\s-\s.*"
        name = re.sub(regex,'',x)
        return name
    
    # row 의 'bs_time'열을 읽고 회계연도를 의미하는 FY열과 회계분기를 의미하는 Quarter를 생성하는 함수 
    def ending_period_extract(self, x) : 
        try : 
            x = datetime.strptime(x, '%Y%d/%m')
            return x
        except : return 

    def report_type_generator(self,x) : 
        try : 
            if x.lower() == 'quarter' : return 'Q'
            else : return 'A'
        except : return np.nan

    # 영문국가명을 넣으면 헤브론스타 국가코드를 반환하는 함수
    def hb_nation_code(self, x) : 
        hebronstar_code = {'Japan':'JPN', 'Hong Kong, China' : 'HKG','Malaysia' : 'MYS','South Korea' : 'KOR',
                        'Singapore':'SGP','Thailand':'THA','Vietnam':'VNM','Indonesia':'IDN',
                        'India':'IND','United States':'USA','Spain':'ESP','Switzerland':'CHE',
                        'Australia':'AUS','United Kingdom':'GBR', 'France':'FRA','Italy':'ITA',
                        'Germany':'DEU','Netherlands':'NLD','Mexico':'MEX','Colombia':'COL','Canada':'CAN'}         
        try : result = hebronstar_code[x]
        except : 
            print("잘못된 국가 이름을 넣었습니다. 국가이름을 표준화 시켜서 넣으십시오.")
            print("ex) korea -> South Korea , hongkong -> Hong Kong, China")
        return 

    # 대응표 주소(mapping_sheet_dir)와 InvestingDotcom 수집코드로 크롤링한 원본데이터(df)를 인풋으로 넣으면, 
    # 대응표를 마탕으로 원하는 데이터 프레임을 추출하는 함수 

    def matching_process(self) : 
        for order, field in enumerate(self.processed.columns) : 
            if field == '현금및예치금액' : 
                try : 
                    self.processed.loc[:,field] = self.data.loc[:,self.processed[field]]
                    continue
                except : 
                    self.processed.loc[:,field] = self.data.loc[:,self.mapping_dic_alternative[field]]
                    continue

            try : self.processed.loc[:,field] = self.data.loc[:,self.mapping_dic[field]]
            except : pass
        data_size = len(self.processed['영문기업명'].unique())
        print(f'인베스팅 닷컴에서 수집한 원본 데이터가 클렌징 되어 총 {data_size}의 기업에 대한 재무제표가 추출 되었습니다.')
        return self.processed
    

    
    # 앞의 메소드들을 cleanse메소드 안에 순서대로 모두 넣는다. 
    # originalfile_dir : 'investingDotcom수집코드'로 크롤링된 데이터 원본주소 
    # mapping_sheet_dir : 대응표 주소 
    # 통화코드결측대체 : 통화코드 결측치를 최빈값으로 대체 하는지 선택하는 옵션. 
    # 인베스팅 닷컴에서 종종 재무제표에서 어떤 통화단위로 적었는지 안나오는 경우가 종종 있다. 
    # 통화코드결측대체는 어떤 통화단위로 적었는지 안 나올때, 여태까지 수집된 데이터를 보고 그나라 상장사들이 
    # 주로 표기하는 통화단위로 null값을 대체(imputation) 하는 코드이다. 
        