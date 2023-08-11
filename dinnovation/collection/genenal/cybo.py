import pandas as pd
import chromedriver_autoinstaller
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC

class information:
    def __init__(self):
        self.print_information = """
        The function is described below. \n
        The main class in the library is cybo_extract. \n
        DriverSettings() is a function that sets the driver. \n
        collect() is collect data.
        """

class cybo_extract:
    def __init__(self):
        self.url = lambda x: f"https://www.cybo.com/UA/kiev-kiev/hotels-&-travel/?p={x}"

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

    def collect(self):
        link_lst = []
        map_lst = []
        for idx in range(1, 11):
            self.driver.get(self.url(idx))
            div_lst = self.driver.find_element(By.XPATH, '//*[@id="bottom"]')
            href_lst = div_lst.find_elements(By.TAG_NAME, 'a')
            for href in href_lst:
                link = href.get_attribute("href")
                if "UA-biz" in link and link not in link_lst: link_lst.append(link)
                elif "r/biz" in link and link not in map_lst: map_lst.append(link)
        self.company_df = pd.DataFrame()
        for link in tqdm(link_lst):
            self.driver.get(link)
            company_name = self.driver.find_element(By.XPATH, '//*[@id="content"]/div[2]/div[1]/div/div/div/h1').text
            company_address = self.driver.find_element(By.XPATH, '//*[@id="content"]/div[2]/div[1]/div/div/div/div[2]').text
            left_content = self.driver.find_element(By.XPATH, '//*[@id="left-contact"]').text
            lst_split = left_content.split("\n")
            for idx in range(len(lst_split)):
                if lst_split[idx-1] == "Phone": phone_number = lst_split[idx]
                elif lst_split[idx-1] == "Website ": company_url = lst_split[idx]
            about = self.driver.find_element(By.XPATH, '//*[@id="right_col"]/div[2]/div/div/div[1]/div').text
            ISIC_code = self.driver.find_element(By.XPATH, '//*[@id="right_col"]/div[2]/div/div/div[2]/div[2]/span[2]/span').text
            categories = self.driver.find_element(By.XPATH, '//*[@id="right_col"]/div[2]/div/div/div[2]/div[1]').text
            company_dict = {"company_name" : company_name,
                        "company_address" : company_address,
                        "phone_number" : phone_number,
                        "company_url" : company_url,
                        "about" : about,
                        "ISIC_code" : ISIC_code,
                        "categories" : categories}
            if self.company_df.empty : self.company_df = pd.DataFrame(company_dict, index=[0])
            else: self.company_df = self.company_df.append(company_dict, ignore_index=True)
        self.company_df.to_excel("우크라이나.xlsx", index = False)