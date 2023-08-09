import pandas as pd
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 datos_extact입니다. \n
        DriverSettings()는 크롬 드라이버를 실행하는 함수입니다. \n
        get_data()는 데이터를 추출하여 가공하는 함수입니다. \n
        load()은 데이터를 저장하는 함수입니다.
        """)

class kemenperin_extract:
    def __init__(self, path):
        """
        path에 TableDefaultColumns 엑셀을 넣어주세요.
        """
        self.DefaultDataFrame = pd.DataFrame(columns = list(pd.read_excel(path, sheet_name="일반_columns")["표준_영문컬럼명"]))

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

    def get_data(self, city = 51, page_num = 1):
        """
        데이터를 추출하는 함수 \n
        city는 KEMENPERIN에 접속하여 city number을 확인해주세요. \n
        page_num은 설정해주세요.
        """
        self.driver.get(f"https://kemenperin.go.id/direktori-perusahaan?what=&prov={city}&hal={page_num}")
        tbody = self.driver.find_element(By.TAG_NAME, "tbody")
        tr = tbody.find_elements(By.TAG_NAME, "tr")
        AppendDict = {
            "hb_ntn_cd" : "IDN",
            "acplc_lngg_ntn_nm" : "Indonesia",
            "engls_ntn_nm" : "Indonesia",
            "ntn_lngg_cd_val" : "IDN",
            "acplc_lngg_lngg_nm" : "Indonesia",
            "engls_lngg_nm" : "Indonesia",
            "acplc_lngg_entrp_nm" : None,
            "engls_entrp_nm" : None,
            "acplc_lngg_oln_intrd_cont" : None,
            "acplc_lngg_entrp_intrd_cont" : None,
            "engls_oln_intrd_cont" : None,
            "entrp_rprsn_tlno" : None,
            "acplc_lngg_entrp_addr" : None,
            "acplc_lngg_entrp_dtadd" : None,
            "engls_entrp_addr" : None,
            "engls_entrp_dtadd" : None
        }
        for td in tr:
            d = td.find_elements(By.TAG_NAME, "td")
            number = d[0].text
            information = d[1].text.split("\n")
            product = d[2].text
            company_name = information[0]
            company_address = information[1]
            company_tel = information[2].replace("Telp." , "")
            AppendDict["acplc_lngg_entrp_nm"] = company_name
            AppendDict["engls_entrp_nm"] = company_name
            AppendDict["acplc_lngg_oln_intrd_cont"] = information
            AppendDict["engls_oln_intrd_cont"] = information
            AppendDict["entrp_rprsn_tlno"] = company_tel
            AppendDict["acplc_lngg_entrp_addr"] = company_address
            AppendDict["acplc_lngg_entrp_dtadd"] = company_address
            AppendDict["engls_entrp_addr"] = company_address
            AppendDict["engls_entrp_dtadd"] = company_address
            AppendDataFrame = pd.DataFrame(AppendDict)
            self.DefaultDataFrame = self.DefaultDataFrame.append(AppendDict, ignore_index= True)

    def load(self):
        self.DefaultDataFrame.to_excel("hb_tb_idn_egnin_m.xlsx", index=False)