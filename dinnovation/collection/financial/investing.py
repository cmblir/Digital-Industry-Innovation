import pandas as pd
import numpy as np
import time
from datetime import datetime
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
        The library is divided into two parts. \n
        Investing_Crawler, a library that collects data, Investing_Cleanse, a library that processes data \n
        -------------------------------------------------- ----------------------------------------\n
        The function of Investing_Crawler is shown below. \n
        DriverSettings() is a Selenium Chrome driver settings function. \n
        download_historial() is a function that collects past stock price data. \n
        collect() is a function that collects data from investing.com. \n
        -------------------------------------------------- ----------------------------------------\n
        Investing_Cleanse will proceed as soon as you run the class. \n
        """)

    def country(self):
        print("""
        japan, hong-kong, malaysia, south-korea
        singapore, thailand, vietnam, indonesia
        india ,united-states, spain, switzerland
        australia, united-kingdom, france, italy
        germany, netherlands, mexico, colombia, canada
        """)

class Investing_Crawler:

    def __init__(self, path):
        """
        Please insert the investing column Excel in the path.
        """
        self.base_url = 'https://au.investing.com'
        self.PROFILE_suffix = '-company-profile'
        self.IS_suffix = '-income-statement'
        self.BS_suffix = '-balance-sheet'
        self.CF_suffix = '-cash-flow'
        self.column_listup = pd.read_excel(path)
        # Investing.com account list. Accounting subjects for general corporations, banks, and insurance businesses were different.
        self.all_bs_cols = pd.concat([self.column_listup['BS'],self.column_listup['BANK BS'], self.column_listup['INSURANCE BS']]).dropna()
        self.all_is_cols = pd.concat([self.column_listup['IS'],self.column_listup['BANK IS'], self.column_listup['INSURANCE IS']]).dropna()
        self.all_cf_cols = pd.concat([self.column_listup['CF'],self.column_listup['BANK CF'], self.column_listup['INSURANCE CF']]).dropna()
        self.all_cols = pd.concat([self.column_listup['BS'],self.column_listup['BANK BS'], self.column_listup['INSURANCE BS'],self.column_listup['IS'],self.column_listup['BANK IS'], self.column_listup['INSURANCE IS'],
                            self.column_listup['CF'],self.column_listup['BANK CF'],self.column_listup['INSURANCE CF']]).dropna()

        # Remove the overlapping parts among the account subjects of general corporations, banks, and insurance businesses to use the union without duplicates. 
        # drop_duplicates removes only one of the duplicates
        self.all_cols = self.all_cols.drop_duplicates()
        self.company_links = []

    def DriverSettings(self, Turn_off_warning = False, linux_mode = False) -> None:
        """
        This function sets the driver.
        If linux mode is set to True, collection is possible in the background.
        However, actions such as clicks cannot be taken.
        """
        if Turn_off_warning == True: self.TurnOffWarning()
        chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]  # Check chromedriver version
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--incognito") # incognito mode
        if linux_mode == True: chrome_options.add_argument("--headless") # Display mode without Linux GUI
        chrome_options.add_argument("--no-sandbox") # Prevent access to resources
        chrome_options.add_argument("--disable-setuid-sandbox") # Prevent chrome crashes
        chrome_options.add_argument("--disable-dev-shm-usage") # Prevent out of memory errors
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        try: # Chrome Driver
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)   
        except:
            chromedriver_autoinstaller.install(True)
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        # Prevent WebDruverException Error Designate as an existing driver version

    def download_historial(self, all_atag_maintable, url):
        company_links = []
        for a in all_atag_maintable:
            company_link = a.attrs["href"]
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

    def collect(self, country, official_countryName ,save_dir, isSingapore=False, download_history=False) : 
        first_url = f'https://au.investing.com/equities/{country.lower()}'
        self.driver.get(first_url)
        time.sleep(8)
        breakpoint()
        # Replace the stocks displayed on the page with 'all stocks'
        try : 
            if isSingapore == False : 
                select_box = self.driver.find_element(By.XPATH,'//*[@id="index-select"]/div[1]').click()
                select_all_stock = self.driver.find_element(By.XPATH,'//*[@id="index-select"]/div[2]/div/div/div[1]').click()
            else : 
                pass
            time.sleep(5)

        # Avoid errors. If an error occurs due to an advertisement pop-up window, use except to avoid the error
        except : 
            # Close ad pop-up window 
            self.driver.find_element(By.XPATH, '//*[@id="PromoteSignUpPopUp"]/div[2]/i').click()

            # Singapore does not have a tab to select all stock. So, the part where you click the all stocks button
            # skip over
            if isSingapore == False : 
                select_box = self.driver.find_element(By.XPATH,'//*[@id="stocksFilter"]').click()
                select_all_stock = self.driver.find_element(By.XPATH,'//*[@id="all"]').click()
            else : 
                pass
            time.sleep(5)

        # Investing.com is a dynamic page.
        # Scroll all the way down once to use BeautifulSoup.
        SCROLL_PAUSE_SEC = 3

        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            
            try:
                some_tag = self.driver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div/div/div[2]/main/div[3]/div[2]/div')
                action = ActionChains(self.driver)
                action.move_to_element(some_tag).perform()
                some_tag.click()
            except NoSuchElementException: break
            # scroll down to the end
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait for SCROLL_PAUSE_SEC
            time.sleep(SCROLL_PAUSE_SEC)
            
            # Get scroll height back after scrolling down
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        time.sleep(20)
        soup = BeautifulSoup(self.driver.page_source,"html.parser")

        # Get the stock table.
        maintable = soup.find('div', {'data-test': 'dynamic-table'})

        # From main stock table to company page
        all_atag_maintable = maintable.find_all('a')

        # If you use soup's find method to get it, not only the link but also other information will be brought back.
        # Therefore, only pure href links are extracted with the for statement below and stored in company_links.
        for a in all_atag_maintable:
            company_link = a.attrs["href"]
            self.company_links.append(company_link)

        # Version 3
        if download_history == True: self.download_historial(all_atag_maintable, first_url)
        else: pass
        wait_time = 3 


        # Remove the overlapping parts among the account subjects of general corporations, banks, and insurance businesses to use the union without duplicates.
        # drop_duplicates removes only one of the duplicates, then lists
        all_cols = list(self.all_cols.drop_duplicates())
        all_cols = all_cols+['is_unit','bs_unit','cf_unit','is_time','bs_time','cf_time','report_type','company_name','industry_info','sector_info',
                            'address_info','phone_info','fax_info','webpage_info','source','gathering_time','PIC']

        # Add company name and general information to all_cols.

        self.result_df = pd.DataFrame()
        crawling_failed_companies = []
        for company in self.company_links : 

            profile_url = self.base_url+company+self.PROFILE_suffix
            bs_url = self.base_url+company+self.BS_suffix
            is_url = self.base_url+company+self.IS_suffix
            cf_url = self.base_url+company+self.CF_suffix

            try : 
                # If you look at company_link, there are companies with unusual links that have 'cid=' attached to the end of the address. These companies need to be dealt with.
                if company.__contains__('cid=') : 
                    index = company.find('?')
                    profile_url =  self.base_url+company[:index]+self.PROFILE_suffix+company[index:]
                    bs_url = self.base_url+company[:index] +self.BS_suffix+company[index:]
                    is_url = self.base_url+company[:index] +self.IS_suffix+company[index:]
                    cf_url = self.base_url+company[:index] +self.CF_suffix+company[index:]

                ############## Company General Information #############
                # company profile page
                self.driver.get(profile_url)
                time.sleep(1)

                # description
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

                # remove special characters
                phone_info = phone_info.replace('\n','')
                phone_info = phone_info.replace('Phone','')
                fax_info = fax_info.replace('\n','')
                fax_info = fax_info.replace('Fax','')
                webpage_info = webpage_info.replace('\n','')
                webpage_info = webpage_info.replace('Web','')

                ############## Corporate financial information #########
                # Quarterly Income Statement
                self.driver.get(is_url)
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break

                df_income_Q = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_is_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text            # Currency unit (Unit)
                company_name = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[1]/h1').text             # extract company name

                # income statement dataframe preprocess
                df_income_Q = df_income_Q.T
                df_income_Q = df_income_Q.rename(columns = df_income_Q.iloc[0])        # set account subject as column name
                df_income_Q = df_income_Q.iloc[1:, :]                                # Delete row 0 and leave only the rest.

                df_income_Q['is_unit'] = df_is_unit                 # insert income statement currency into dataframe
                df_income_Q['is_time']= df_income_Q.index                  # Add 'time' to the dataframe
                df_income_Q['report_type'] = 'Quarter'                         # Mark the data frame as a quarterly report
                df_income_Q['company_name'] = company_name                     # insert company name into dataframe
                df_income_Q=df_income_Q.reset_index(drop=True) 

                # yearly Income Statement. 
                try : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[8]/div[1]/a[1]').click()
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
                df_income_A = df_income_A.rename(columns = df_income_A.iloc[0])        # set account subject as column name
                df_income_A = df_income_A.iloc[1:, :]                                # Delete row 0 and leave only the rest.

                df_income_A['is_unit'] = df_is_unit                 # insert income statement currency into dataframe
                df_income_A['is_time']= df_income_A.index                  # Add 'time' to the dataframe
                df_income_A['report_type'] = 'Annual'                          # Mark the data frame as a quarterly report
                df_income_A['company_name'] = company_name                     # insert company name into data frame
                df_income_A=df_income_A.reset_index(drop=True) 


                # balance data
                self.driver.get(bs_url)
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order


                # quarterly balance sheet
                df_balance_Q = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_bs_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text           # Extract currency unit (Unit)
                time.sleep(wait_time)
                # balance sheet dataframe preprocess
                df_balance_Q = df_balance_Q.T
                df_balance_Q = df_balance_Q.rename(columns = df_balance_Q.iloc[0])    # set account subject as column name
                df_balance_Q = df_balance_Q.iloc[1:, :]                              # Delete row 0 and leave only the rest.
                ##df_balance = df_balance[bs_targets]
                df_balance_Q['bs_unit'] = df_bs_unit                  # insert cash flow currency unit into dataframe
                df_balance_Q['bs_time']= df_balance_Q.index
                #df_balance_Q['report_type'] = 'Quarter'                         # Mark the data frame as a quarterly report
                #df_balance_Q['company_name'] = company_name                     # insert company name into data frame
                df_balance_Q=df_balance_Q.reset_index(drop=True)


                # yearly balance sheet 
                try : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[8]/div[1]/a[1]').click() # Click the Annual button
                except :
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[9]/div[1]/a[1]').click() # Click the Annual button
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break

                df_balance_A = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_bs_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text           # Extract currency unit (Unit)
                time.sleep(wait_time)
                # balance sheet dataframe preprocess
                df_balance_A = df_balance_A.T
                df_balance_A = df_balance_A.rename(columns = df_balance_A.iloc[0])     # set account subject as column name
                df_balance_A = df_balance_A.iloc[1:, :]                              # Delete row 0 and leave only the rest.
                ##df_balance = df_balance[bs_targets]
                df_balance_A['bs_unit'] = df_bs_unit                  # insert cash flow currency unit into dataframe
                df_balance_A['bs_time']= df_balance_A.index
                #df_balance_A['report_type'] = 'Annual'                          # Mark the data frame as an annual report
                #df_balance_A['company_name'] = company_name                     # insert company name into data frame
                df_balance_A = df_balance_A.reset_index(drop=True)

                # cash flow
                self.driver.get(cf_url)
                time.sleep(2)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break    

                # quarterly cashflow 
                df_cash_flow_Q = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_cf_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text           # Extract currency unit (Unit)
                time.sleep(wait_time)

                df_cash_flow_Q = df_cash_flow_Q.T 
                df_cash_flow_Q = df_cash_flow_Q.rename(columns = df_cash_flow_Q.iloc[0])# set account subject as column name
                df_cash_flow_Q = df_cash_flow_Q.iloc[1:, :]                             # Delete row 0 and leave only the rest.
                df_cash_flow_Q['cf_unit'] = df_cf_unit                     # insert cash flow currency unit into dataframe
                df_cash_flow_Q['cf_time']= df_cash_flow_Q.index.get_level_values(0)  # cf는 period_ending, period_length Using two multi-indexes  
                df_cash_flow_Q = df_cash_flow_Q.reset_index(drop=True)

                # yearly cashflow 
                try : 
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[8]/div[1]/a[1]').click() # Click the Annual button
                except :                         
                    self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[9]/div[1]/a[1]').click() # Click the Annual button
                time.sleep(wait_time)

                for order, table in enumerate(pd.read_html(self.driver.page_source)) : 
                    if len(table) > 20 :
                        table_num = order
                        break        

                df_cash_flow_A = pd.read_html(self.driver.page_source)[table_num].dropna()
                df_cash_flow_unit = self.driver.find_element(By.XPATH,'//*[@id="leftColumn"]/div[10]').text            # Extract currency unit (Unit)

                #cash flow dataframe preprocess
                df_cash_flow_A = df_cash_flow_A.T 
                df_cash_flow_A = df_cash_flow_A.rename(columns = df_cash_flow_A.iloc[0])# set account subject as column name
                df_cash_flow_A = df_cash_flow_A.iloc[1:, :]                             # Delete row 0 and leave only the rest.
                df_cash_flow_A['cf_unit'] = df_cf_unit                     # insert cash flow currency unit into dataframe
                df_cash_flow_A['cf_time']= df_cash_flow_A.index.get_level_values(0)  # cf는 period_ending, period_length Using two multi-indexes
                df_cash_flow_A = df_cash_flow_A.reset_index(drop=True) 

                # Integrate column by batch.
                # company_df_A is a business report obtained by pressing the 'Annual' button
                company_df_A = pd.concat([df_balance_A, df_income_A, df_cash_flow_A],axis=1) # Indicate that it is a business report in the data frame
                company_df_A['industry_info'] = industry_info
                company_df_A['sector_info'] = sector_info
                company_df_A['address_info'] = address_info
                company_df_A['phone_info'] = phone_info
                company_df_A['fax_info'] = fax_info
                company_df_A['webpage_info'] = webpage_info
                company_df_A['source'] = 'https://au.investing.com'+company+self.IS_suffix
                company_df_A['PIC'] = 'Nicholas'
                company_df_A['gathering_time'] = datetime.today().strftime("%Y-%m-%d")
                
                # company_df_Q is the quarterly report obtained by pressing the 'Quarter' button
                company_df_Q = pd.concat([df_balance_Q, df_income_Q, df_cash_flow_Q],axis=1)# Mark the data frame as a quarterly report
                company_df_Q['industry_info'] = industry_info
                company_df_Q['sector_info'] = sector_info
                company_df_Q['address_info'] = address_info
                company_df_Q['phone_info'] = phone_info
                company_df_Q['fax_info'] = fax_info
                company_df_Q['webpage_info'] = webpage_info
                company_df_Q['source'] = 'https://au.investing.com'+company+self.IS_suffix
                company_df_Q['PIC'] = 'Nicholas'
                company_df_Q['gathering_time'] = datetime.today().strftime("%Y-%m-%d")

                
                blank = pd.DataFrame(columns = all_cols)
                for code in blank.columns : 
                # Extract necessary information with the ifrs code of the correspondence table.
                    try : 
                        blank[code] = company_df_A[code] 
                    except : 
                        pass  

                self.result_df = self.result_df.append(blank)
                blank = pd.DataFrame(columns = all_cols)

                for code in blank.columns : 
                # Extract necessary information with the ifrs code of the correspondence table.
                    try : 
                        blank[code] = company_df_Q[code] 
                    except : 
                        pass  


                self.result_df = self.result_df.append(blank)

            except :
                # If an error occurs due to lack of financial information, add general information as well.
                ############## Company General Information #########
                #company profile page
                self.driver.get(profile_url)
                time.sleep(1)

                #  description
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                try : 
                    company_name = soup.find('h2').text.replace('Company Profile',"")
                    #  description
                    desciption_info = soup.find('div', attrs = {'class' : 'companyProfileBody'}).text
                    desciption_info = desciption_info.replace('\n','')
                except : 
                    print(f"A company without a profile {profile_url}")
                    print(f"A company without a profile {company}")
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
                blank['gathering_time'] = datetime.today().strftime("%Y-%m-%d")


                for code in blank.columns : 
                    # Extract necessary information with the ifrs code of the correspondence table.
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
        
        
    def __init__(self, originalfile_dir, mapping_sheet_dir, Replace_missing_currency_code = True):
        self.data = pd.read_excel(originalfile_dir)
        # Remove rows without company name
        self.data = self.data.dropna(subset = ['company_name'])
        # Add a column called 'currency' corresponding to the currency code to data
        self.data['currency'] = self.data['bs_unit'].apply(lambda x : self.currency_extract(x))
        # Add column 'unit' to self.data
        self.data['unit'] = self.data['bs_unit'].apply(lambda x : self.unit_change(x))
        # Columns containing numeric data in the original data are from the 0th column to the 141st column.
        # Before changing the unit of numeric data with self.data['unit'], sometimes null values are entered as '-', so remove them first.
        for name in self.data.columns[:142] : self.data[name]= self.data[name].replace('-',np.nan)
        # Change units for numeric data.
        # Columns 0 to 113 contain numeric data
        for name in self.data.columns[:142] : self.data[name]= self.data[name].astype('float')*self.data['unit']
        # Add a column called 'stock_code' by extracting the ticker from the company name.
        self.data['stock_code'] = self.data['company_name'].apply(lambda x : self.ticker_extract(x))
        # Since we extracted the ticker from the company name, remove the ticker.
        self.data['company_name'] = self.data['company_name'].apply(lambda x : self.ticker_delete(x))
        # Process bs_time into date_time format to generate fiscal year and fiscal quarter
        # Add a column called 'ending' period.
        self.data['ending_period'] = self.data['bs_time'].apply(lambda x : self.ending_period_extract(x))
        # remove missing values
        # Remove missing values without ending_perod before extracting the fiscal year and fiscal quarter.
        self.data = self.data.dropna(subset=['ending_period'])
        # Fill Hebron star country code column
        try: self.data['hb_nation_code'] = self.data['Country'].apply(lambda x : self.hb_nation_code(x))
        except KeyError:
            country_name = originalfile_dir.split(".xlsx")[-1]
            self.data['hb_nation_code'] = self.hb_nation_code(country_name)
        # Replace or remove data without a currency code with a frequent value.
        if Replace_missing_currency_code == True : 
            try: self.data['currency'] = self.data['currency'].fillna(value = self.data['currency'].mode()[0])
            except: print("currency is miss")
            # Fill in the fields without a currency code with the most frequently used currency code for the country.
        else : self.data = self.data.dropna(subset=['currency'])
            # If currency code missing substitution is False, rows with missing values are removed.
        # Create a Fiscal year column corresponding to the fiscal year using ending_period
        self.data['Fiscal year'] = self.data['ending_period'].apply(lambda x : x.year)
        # Creating an ending_period column corresponding to the settlement date using ending_period

        self.data['ending_period'] = self.data['ending_period'].apply(lambda x : x.strftime("%Y%m%d"))
        # Processing to extract illiquid assets
        self.data['Total Assets - Total Current Assets'] = self.data['Total Assets']-self.data['Total Current Assets']
        # Process for extracting total liabilities and assets
        self.data['Total Liabilities + Total Equity'] = self.data['Total Liabilities']+self.data['Total Equity']
        # report type
        self.data['report_type'] = self.data['report_type'].apply(lambda x : self.report_type_generator(x))
        self.mapping_sheet = pd.read_excel(mapping_sheet_dir)
        self.mapping_dic = self.mapping_sheet.set_index('2022 field name').T.to_dict('index')['corresponding field name 1']
        self.mapping_dic_alternative = self.mapping_sheet.set_index('2022 field name').T.to_dict('index')['corresponding field name 2']
        # After creating an empty data frame called procssed, the original data (df) crawled with 'InvestingDotcom collection code' is retrieved with the loop statement below.
        # standardize
        self.processed = pd.DataFrame(columns = self.mapping_dic.keys())
        
    # Function to extract currency identification code from bs_unit 
    def currency_extract(self, x) :
        world_currency = ['HKD', 'EUR', 'AUD', 'KRW', 'NZD', 'PHP', 'USD', 'DKK', 'TRY', 'CAD', 'CLP','INR', 'EGP', 'NOK', 'MYR', 'MXN', 
                        'CHF', 'GBP', 'SGD', 'ARS', 'THB', 'JPY', 'CNY','IDR','VND', 'COP']
        for currency in world_currency : 
            try : 
                if str(currency) in x : return currency 
                else : continue
            except : return np.nan
        return np.nan    
    
    # Function to extract numeric units used in financial statements
    def unit_change(self,x) : 
        try : 
            if "Millions" in x : unit =  1000000
            elif "Billions" in x : unit = 1000000000
            else : unit = 0
        except : unit = x
        return unit
    
    # Function to extract ticker from company name
    def ticker_extract(self,x) : 
        ticker = re.findall('\(([^)]+)',x)
        ticker = ticker[0] # Since findall returns a list, only elements are extracted using indexing
        return ticker
    
    # Function to remove ticker from company name
    def ticker_delete(self,x) : 
        regex = "\(.*\)|\s-\s.*"
        name = re.sub(regex,'',x)
        return name
    
    # A function that reads the row's 'bs_time' column and creates a FY column for the fiscal year and a Quarter for the fiscal quarter.
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

    # A function that returns the Hebron Star country code if an English country name is entered
    def hb_nation_code(self, x) : 
        hebronstar_code = {'japan':'JPN', 'hong-kong' : 'HKG','malaysia' : 'MYS','south-korea' : 'KOR',
                        'singapore':'SGP','thailand':'THA','vietnam':'VNM','indonesia':'IDN',
                        'india':'IND','united-states':'USA','spain':'ESP','switzerland':'CHE',
                        'australia':'AUS','united-kingdom':'GBR', 'france':'FRA','italy':'ITA',
                        'germany':'DEU','netherlands':'NLD','mexico':'MEX','colombia':'COL','canada':'CAN'}         
        try :
            result = hebronstar_code[x]
            return result
        except : 
            print("You entered the wrong country name. Please enter standardized country names.")
            print("ex) korea -> South Korea , hongkong -> Hong Kong, China")
            return
        

    # If you input the correspondence table address (mapping_sheet_dir) and the original data (df) crawled with the InvestingDotcom collection code as input,
    # Function to extract the desired data frame based on the correspondence table 

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
        print(f'The original data collected from Investing.com was cleansed to extract financial statements for a total of {data_size} companies.')
        return self.processed
    

    
    # Put all the preceding methods in order in the cleanse method.
    # originalfile_dir : Source address of data crawled by 'investingDotcom collection code'
    # mapping_sheet_dir: mapping table address
    # Replace missing currency code: Option to select whether to replace missing currency code with the most frequent value.
    # Investing.com often does not show which currency unit is written in the financial statement.
    # When currency code missing replacement is not available in which currency unit, listed companies in that country look at the data collected so far
    # This is the code to imputation the null value with the currency unit that is mainly expressed.