import pandas as pd
import time
import psycopg2
import chromedriver_autoinstaller
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        The SHAREOUTSTANDING library collects market cap data. \n
        DriverSettings() is a Selenium Chrome driver settings function. \n
        get_company() is a function that retrieves a ticker from our US company database and stores its value. \n
        collect() is a function that collects data from shareoutstanding sites. \n
        """)

class SHAREOUTSTANDING:
    def __init__(self):
        self.url = "https://www.sharesoutstandinghistory.com/"

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
        
    def get_company(self, host, database, user, password):
        conn = psycopg2.connect(
            host = host,
            database = database,
            user = user,
            password = password
        )
        query = "SELECT * FROM tb_hb_usa_plcfi_d"
        df = pd.read_sql(query, conn)

        self.company_lst = [i for i in list(df["lstng_cd"].unique()) if i is not None]


    def collect(self):
        self.driver.get(self.url)
        self.driver.maximize_window()
        df = pd.DataFrame()
        for idx in tqdm(self.company_lst):
            search_bar = self.driver.find_element(By.XPATH, '//*[@id="symbol"]')
            search_bar.clear()
            search_bar.send_keys(idx+"\n")
            time.sleep(2)
            try: self.driver.find_element(By.XPATH, '//*[@id="baltimore-button-no"]').click()
            except: pass
            try: 
                error_site = self.driver.find_element(By.XPATH, '/html/body/center/div[4]/div[2]/div[1]/h1').text
                if error_site == "404 File Not Found": continue
            except: pass
            time.sleep(2)
            table = self.driver.find_element(By.XPATH, '/html/body/center/div[4]/div[2]/div[2]/table[1]/tbody/tr[2]/td/center/table')
            table_html = table.get_attribute('outerHTML')
            append_df = pd.read_html(table_html)[0].T
            df = df.append(append_df, ignore_index=True)
        return df
