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
        A description of the function is given below. \n
        The main class within the library is datos_extact. \n
        DriverSettings() is a function that launches the chrome driver. \n
        get_data() is a function that extracts and processes data. \n
        load() is a function that stores data.
        """)

class kemenperin_extract:
    def __init__(self, path):
        """
        Please put TableDefaultColumns Excel in the path.
        """
        self.DefaultDataFrame = pd.DataFrame(columns = list(pd.read_excel(path, sheet_name="일반_columns")["표준_영문컬럼명"]))

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

    def get_data(self, city = 51, page_num = 1):
        """
        Function to extract data \n
        For the city, access KEMENPERIN and check the city number. \n
        Please set page_num.
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