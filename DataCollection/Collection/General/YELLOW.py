import pandas as pd
import chromedriver_autoinstaller
import warnings
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning) 

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        The function is described below. \n
        The main class in the library is opencorporates_extract. \n
        DriverSettings() is a function that sets the driver. \n
        """)


class yellow_extract:
    def __init__(self):
        self.url = lambda x: x
        self.country_dict = {
            "파키스탄" : "https://www.businesslist.pk/",
            "아르헨티나" : "https://www.arempresas.com/",
            "칠레" : "https://www.yelu.cl/",
            "브라질" : "https://www.brazilyello.com/",
            "카자흐스탄" : "https://www.kazakhstanyp.com/",
            "벨라루스" : "https://www.yelo.by/",
            "르완다" : "https://www.rwandayp.com/",
            "탄자니아" : "https://www.tanzapages.com/", 
            "우간다" : "https://www.yellow.ug/",
            "남아공" : "https://www.yellosa.co.za/",
            "일본" : "https://www.japanyello.com/",
            "홍콩" : "https://www.yelo.hk/",
            "말레이시아" : "https://www.businesslist.my/",
            "싱가포르" : "https://www.yelu.sg/",
            "태국" : "https://www.thaiyello.com/",
            "베트남" : "https://www.vietnamyello.com/",
            "인도네시아" : "https://www.indonesiayp.com/",
            "인도" : "https://www.yelu.in/",
            "멕시코" : "https://www.yelo.com.mx/",
            "네덜란드" : "https://www.yelu.nl/",
            "호주" : "https://www.australiayp.com/",
            "스위스" : "https://www.swissyello.com/"
        }

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


    def Extact(self, country_name):
        CompaniesInformation = pd.DataFrame()
        self.driver.get(self.url(self.country_dict[country_name]))
        location_lst = self.driver.find_element(By.XPATH, '/html/body/section[2]/ul')
        locations = location_lst.find_elements(By.TAG_NAME, 'a')
        location_link_lst = []
        for location in locations:
            location_link = location.get_attribute("href")
            if "location" in location_link and location_link not in location_link_lst: location_link_lst.append(location_link)
        for link in location_link_lst:
            self.driver.get(link)
            find_max_page_number = self.driver.find_element(By.XPATH, '//*[@id="listings"]')
            max_page_number = max([int(i.text) for i in find_max_page_number.find_elements(By.TAG_NAME, 'a') if i.get_attribute("class") == "pages_no"])
            for idx in tqdm(range(1, max_page_number)):
                try:
                    if idx == 1: pass
                    else: self.driver.get(self.url + f"/{idx}")
                    href_lst = self.driver.find_element(By.XPATH, '//*[@id="listings"]')
                    company_link_lst = []
                    for hrefs in href_lst.find_elements(By.TAG_NAME, "a"):
                        href = hrefs.get_attribute("href")
                        if "company" in href and "reviews" not in href and href not in company_link_lst: company_link_lst.append(href)
                    for company_link in company_link_lst:
                        self.driver.get(company_link)
                        div_lst = self.driver.find_elements(By.TAG_NAME, "div")
                        lst = []
                        for div in div_lst:
                            if div.get_attribute('class') == 'info': lst.append(div.text)
                        company_name = lst[0].split("\n")[1]
                        append_df = pd.DataFrame({company_name : lst[1:]})
                        CompaniesInformation = pd.concat([CompaniesInformation, append_df], axis=1)
                        print(f"appended {company_name}")
                except:
                    pass