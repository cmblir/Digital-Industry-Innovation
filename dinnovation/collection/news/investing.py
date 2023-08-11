import pandas as pd
import chromedriver_autoinstaller
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class investing_news_extract:
    def __init__(self):
        pass

    def set_columns(self):
        """
        Column setting for news
        """
        self.news_append_columns = ["entrp_reltn_tdngs_url", "entrp_reltn_tdngs_subjc", "entrp_reltn_tdngs_dtl_cont"]
        self.new_news_columns = ["industry_reltn_tdngs_url", "industry_reltn_tdngs_subjc", "industry_reltn_tdngs_dtl_cont"]

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

    def input_dataframe(self, df) -> pd.DataFrame:
        """
        Set the data frame to be collected
        """
        self.df = df

    def get_information(self):
        """
        Collect news from Investing based on the listing code of the data frame.
        """
        symbol = list(set(self.df["lstng_cd"]))
        self.news_dict = {}
        for idx in tqdm(symbol):
            html = f"https://www.investing.com/search/?q={idx}&tab=news"
            self.driver.get(html)
            article_lst = []
            for dix in self.driver.find_elements(By.XPATH, "//div[contains(@class, 'articleItem')]"):
                article = dix.find_element(By.TAG_NAME, 'a').get_attribute("href")
                if "news/" in article: article_lst.append(article)
            self.news_dict[idx] = article_lst 

    def change_length(self):
        max_len = max(map(len, self.news_dict.values()))

        # fit all lists to the length of the longest list
        for key, value in self.news_dict.items():
            if len(value) < max_len:
                self.news_dict[key] = value + [None] * (max_len - len(value))
        self.df = pd.DataFrame(self.news_dict)

    def collect(self):
        """
        Collect news information by accessing the url address collected from investing
        """
        self.base_df = pd.DataFrame()
        for col in tqdm(list(self.df.columns)):
            symbol = col
            for length in range(len(self.df[col])):
                try:
                    url = str(self.df[col][length])
                    if not url == "nan":
                        self.driver.get(url)
                        article_header = self.driver.find_element(By.CLASS_NAME, 'articleHeader').text
                        article_page = self.driver.find_element(By.XPATH, "//div[contains(@class, 'articlePage')]")
                        article_date = self.driver.find_element(By.CLASS_NAME, 'contentSectionDetails').text
                        articles = []
                        for article in article_page.find_elements(By.TAG_NAME, 'p'):
                            articles.append(article.text)
                        article_joined = " ".join(articles)
                    infor_dict = {
                        "symbol" : symbol,
                        "article_header" : article_header,
                        "article_date" : article_date,
                        "article" : article_joined,
                        "article_url" : url
                    }
                    append_df = pd.DataFrame(infor_dict, index=[0])
                    self.base_df = self.base_df.append(append_df, ignore_index=True)
                except: pass

    def merge_data(self, path):
        """
        Combining data collected from url addresses
        """
        self.df = self.df.drop(columns=["Unnamed : 0"])
        self.df = self.df.dropna()
        for idx, jdx in zip(self.news_append_columns, self.new_news_columns):
            self.df[jdx] = self.df[idx]
        self.df = self.df.drop(columns="article_date")
        for idx in tqdm(range(len(self.base_df))):
            symbol = self.base_df["lstng_cd"][idx]
            append_df = self.df[self.df["lstng_cd"] == symbol]
            if not append_df.empty:
                if str(self.base_df.loc[idx, list(append_df.columns)]["industry_reltn_tdngs_url"]) == "nan":
                    self.base_df.loc[idx, list(append_df.columns)] = append_df.values[0]
                else:
                    print(str(self.base_df.loc[idx, list(append_df.columns)]["industry_reltn_tdngs_url"]))
        self.base_df.to_excel(path, index=False)