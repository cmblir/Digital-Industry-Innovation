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
        SHAREOUTSTANDING 라이브러리는 market cap 데이터를 수집합니다. \n
        ------------------------------------------------------------------------------------------\n
        DriverSettings()은 셀레니움 크롬 드라이버 세팅 함수입니다. \n
        get_company()는 자사의 미국 기업 데이터베이스에서 ticker를 검색하여 해당 값을 저장하는 함수입니다. \n
        collect()은 shareoutstanding 사이트에서 데이터를 수집하는 함수입니다. \n
        ------------------------------------------------------------------------------------------\n
        Investing_Cleanse는 클래스를 실행시키면 바로 진행이 됩니다. \n
        """)

class SHAREOUTSTANDING:
    def __init__(self):
        self.url = "https://www.sharesoutstandinghistory.com/"

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
