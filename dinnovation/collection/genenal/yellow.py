import pandas as pd
import chromedriver_autoinstaller
import warnings
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning) 

class information:
    def __init__(self):
        self.print_information = """
        The function is described below. \n
        The main class in the library is opencorporates_extract. \n
        DriverSettings() is a function that sets the driver. \n
        """

    def __repr__(self) -> str:
        return str(self.print_information)
        


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
    def __repr__(self):
        return str(self.country_dict.keys())

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

    def moment_save(self, number, country_name):
        self.companies_info.to_csv(f"{country_name}_{number}.csv", index=False)
        return f"{country_name} is passed"

    def extract(self, country_name, moment_stop = False):
        self.companies_info = pd.DataFrame()
        self.driver.get(self.url(self.country_dict[country_name]))

        location_list = self.driver.find_element(By.XPATH, '/html/body/section[2]/ul')
        locations = location_list.find_elements(By.TAG_NAME, 'a')
        self.location_links = [location.get_attribute("href") for location in locations if "location" in location.get_attribute("href")]

        for link in set(self.location_links):
            self.driver.get(link)
            find_max_page_number = self.driver.find_element(By.XPATH, '//*[@id="listings"]')
            max_page_number = max(int(i.text) for i in find_max_page_number.find_elements(By.TAG_NAME, 'a') if i.get_attribute("class") == "pages_no")
            
            for idx in tqdm(range(1, max_page_number + 1)):
                if idx > 1: self.driver.get(f"{link}/{idx}")
                
                if moment_stop == True and idx % 100 == 0:
                    self.moment_save(idx, country_name)
                    break
                    
                href_list = self.driver.find_element(By.XPATH, '//*[@id="listings"]')
                company_links = [href.get_attribute("href") for href in href_list.find_elements(By.TAG_NAME, "a") if "company" in href.get_attribute("href") and "reviews" not in href.get_attribute("href")]

                for company_link in set(company_links):
                    self.driver.get(company_link)
                    left_list = self.driver.find_element(By.XPATH, '//*[@id="left"]')
                    div_list = left_list.find_elements(By.TAG_NAME, "div")
                    
                    try:
                        info_texts = [div.text for div in div_list if div.get_attribute('class') == ['info']]
                        if not info_texts:
                            continue
                        company_name = info_texts[0].split("\n")[1]
                        append_df = pd.DataFrame({company_name: info_texts[1:]})
                        self.companies_info = pd.concat([self.companies_info, append_df], axis=1)
                        print(f"appended {company_name}")
                    except Exception as e:
                        print(f"Error while processing {company_link}: {e}")
                        continue
        self.driver.close()

        return self.companies_info


