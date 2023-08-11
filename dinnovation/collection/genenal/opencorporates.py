import chromedriver_autoinstaller
import warnings
import pandas as pd
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning) 

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class within the library is opencorporates_extract. \n
        DriverSettings() is a function that sets the driver. \n
        Login() is a function to log in to opencorporates. \n
        ReCounty() is a function that selects a country. \n
        SearchCompanies() is a function that finds companies. \n
        GetInformation() is a function that extracts data. \n
        GetExcel() is a function that stores the extracted data. \n
        """)

class country_name:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        Hong Kong, Malaysia, Singapore, Thailand \n
        Viet Nam, India, United States, Canada \n
        Mexico, Netherlands, Germany, Italy, France \n
        United Kingdom, Australia, Switzerland, Spain
        """)


class opencorporates_extract:
    def __init__(self):
        self.url = "https://opencorporates.com/"
        self.accounturl = "https://opencorporates.com/users/account"
        self.CompaniesInformationUrl = pd.DataFrame(columns=["country", "company name", "url"])
        self.CompaniesInformation = pd.DataFrame()
    
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
    
    def Login(self, id, pw):
        """
        Function to log in opencorporates
        Enter id and pw.
        """
        self.driver.get(self.accounturl)
        time.sleep(3)
        EmailAddress = self.driver.find_element(By.XPATH, '//*[@id="user_email"]')
        EmailPassword = self.driver.find_element(By.XPATH, '//*[@id="user_password"]')
        EmailAddress.send_keys(id)
        EmailPassword.send_keys(pw)
        SIGN = self.driver.find_element(By.XPATH, '//*[@id="new_user"]/div[5]/div/button')
        SIGN.send_keys("\n")
    
    def ReCountry(self, CountryName):
        """
        Function to select country
        Please enter the country name in CountryName.
        """
        self.driver.get(self.url)
        Countries = Select(self.driver.find_element(By.NAME, "jurisdiction_code"))
        Countries.select_by_visible_text(CountryName)
        CountriesApply = self.driver.find_element(By.XPATH, '//*[@id="home-background-circle"]/div/div[2]/div/div[1]/form/div[2]/div[2]/div[3]/button')
        CountriesApply.send_keys("\n")
        ExcludeInactive = self.driver.find_element(By.XPATH, '//*[@id="inactive"]')
        ExcludeInactive.send_keys(" ")
        GoSearch = self.driver.find_element(By.XPATH, '//*[@id="basic_companies_search"]/div[2]/input[2]')
        GoSearch.send_keys("\n")
    
    def SearchCompanies(self, CountryName):
        """
        A function to search for a company and extract the url
        Please enter the country name in CountryName.
        """
        TotalCompanies = int(self.driver.find_element(By.XPATH, '//*[@id="page_container"]/div[2]/div[1]/h2').text.split()[1].replace(",", ""))
        for _ in range(TotalCompanies // 30):
            results = self.driver.find_element(By.ID, "results")
            results = results.find_element(By.TAG_NAME, "ul")
            results = results.find_elements(By.TAG_NAME, "li")
            for result in results:
                Aclass = result.find_elements(By.TAG_NAME, "a")
                CompanyName = Aclass[1].text
                CompanyUrl = Aclass[1].get_attribute("href")
                df = pd.DataFrame({"country" : CountryName, "company name" : CompanyName, "url" : CompanyUrl}, index=[0])
                self.CompaniesInformationUrl = self.CompaniesInformationUrl.append(df, ignore_index=True)
            NextPage = self.driver.find_element(By.XPATH, '//*[@id="results"]/div/div[1]/ul/li[8]/a').get_attribute("href")
            self.driver.get(NextPage)
        self.CompaniesInformationUrl.to_excel(f"finished_{CountryName}_url_opencorporates.xlsx", index = False, engine='xlsxwriter')
    
    def GetInformation(self, url, CountryName):
        """
        A function that searches for companies in url and extracts information
        Please enter the country name in CountryName.
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
        function to store data
        """
        self.CompaniesInformation.to_excel("finished_opencorporates.xlsx", index=False, engine="xlsxwriter")