import chromedriver_autoinstaller
import warnings
import pandas as pd
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from tqdm import tqdm
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning) 

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 opencorporates_extract입니다. \n
        DriverSettings()는 드라이버 세팅을 하는 함수입니다. \n
        Login()은 opencorporates에 로그인하는 함수입니다. \n
        ReCounty()는 국가를 선택하는 함수입니다. \n
        SearchCompanies()는 기업을 찾는 함수입니다. \n
        GetInformation()은 데이터를 추출하는 함수입니다. \n
        GetExcel()은 추출한 데이터를 저장하는 함수입니다. \n
        """)

class opencorporates_extract:
    def __init__(self):
        self.url = "https://opencorporates.com/"
        self.accounturl = "https://opencorporates.com/users/account"
        self.CompaniesInformationUrl = pd.DataFrame(columns=["country", "company name", "url"])
        self.CompaniesInformation = pd.DataFrame()
    
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
    
    def Login(self, id, pw):
        """
        opencorporates에 로그인하는 함수
        id와 pw를 입력하세요.
        """
        self.driver.get(self.accounturl)
        time.sleep(3)
        EmailAddress = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/div[2]/form/div[2]/div/input")
        EmailPassword = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/div[2]/form/div[3]/div/input")
        EmailAddress.send_keys(id)
        EmailPassword.send_keys(pw)
        SIGN = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/div[2]/form/div[5]/div/button")
        SIGN.send_keys("\n")
    
    def ReCountry(self, CountryName):
        """
        국가를 선택하는 함수
        CountryName에 국가명을 입력해주세요.
        """
        self.driver.get(self.url)
        Countries = Select(self.driver.find_element(By.NAME, "jurisdiction_code"))
        Countries.select_by_visible_text(CountryName)
        CountriesApply = self.driver.find_element(By.XPATH, "/html/body/header[1]/div/div[2]/div/div[1]/form/div[2]/div[2]/div[3]/button")
        CountriesApply.send_keys("\n")
        ExcludeInactive = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/div[1]/form/div[3]/div/label/input")
        ExcludeInactive.send_keys(" ")
        # GoSearch = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/div[1]/form/div[2]/input[2]")
        ExcludeInactive.send_keys("\n")
    
    def SearchCompanies(self, CountryName):
        """
        회사를 검색하여 url을 추출하는 함수
        CountryName에 국가명을 입력해주세요.
        """
        TotalCompanies = int(self.driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/h2").text.split()[1].replace(",", ""))
        for _ in range(2):
            results = self.driver.find_element(By.ID, "results")
            results = results.find_element(By.TAG_NAME, "ul")
            results = results.find_elements(By.TAG_NAME, "li")
            for result in results:
                Aclass = result.find_elements(By.TAG_NAME, "a")
                CompanyName = Aclass[1].text
                CompanyUrl = Aclass[1].get_attribute("href")
                df = pd.DataFrame({"country" : CountryName, "company name" : CompanyName, "url" : CompanyUrl}, index=[0])
                self.CompaniesInformationUrl = self.CompaniesInformationUrl.append(df, ignore_index=True)
            NextPage = self.driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/div[2]/div/div[1]/ul/li[8]/a").get_attribute("href")
            self.driver.get(NextPage)
        self.CompaniesInformationUrl.to_excel("finished_url_opencorporates.xlsx", index = False, engine='xlsxwriter')
    
    def GetInformation(self, url, CountryName):
        """
        url내의 회사를 검색하여 정보를 추출하는 함수
        CountryName에 국가명을 입력해주세요.
        """
        self.driver.get(url)
        vcard = self.driver.find_element(By.CLASS_NAME, "vcard")
        CompanyName = vcard.find_element(By.TAG_NAME, "h1").text
        TableName = vcard.find_elements(By.TAG_NAME, "dt")
        TableIndex = vcard.find_elements(By.TAG_NAME, "dd")
        TableDict = {"Country" : CountryName, "Company Name" : CompanyName}
        for table, index in zip(TableName, TableIndex):
            TableDict[table.text] = index.text
        Tabledf = pd.DataFrame(TableDict, index=[0])
        self.CompaniesInformation = self.CompaniesInformation.append(Tabledf, ignore_index = True)

    def GetExcel(self):
        """
        데이터를 저장하는 함수
        """
        self.CompaniesInformation.to_excel("finished_opencorporates.xlsx", index=False, engine="xlsxwriter")