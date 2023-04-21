import pandas as pd
import numpy as np
import chromedriver_autoinstaller
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 BIZIN입니다. \n
        아시아 국가의 경우 url이 다르므로 설정해줘야 합니다. \n
        DriverSettings()은 셀레니움 크롬 드라이버 세팅 함수입니다. \n
        area()는 해당 국가의 기업들 정보를 수집하는 함수입니다. \n
        collect()은 BIZIN 사이트에서 데이터를 수집하는 함수입니다. \n
        """)

class country_name:
    def __init__(self):
        self.print_information()

    def print_information(self):
        self.country_name = {"영국" : "gb",
        "독일" : "de",
        "스위스" : "ch",
        "호주" : "at",
        "이란" : "ie",
        "벨기에" : "be",
        "스페인" : "es",
        "포르투칼" : "pt",
        "프랑스" : "fr",
        "이탈리아" : "it",
        "덴마크" : "dk",
        "네덜란드" : "nl",
        "노르웨이" : "no",
        "핀란드" : "fi",
        "스웨덴" : "se",
        "라트비아" : "lv",
        "에스토니아" : "ee",
        "폴란드" : "pl",
        "벨라루스" : "by",
        "우크라이나" : "ua",
        "룩셈부르크" : "lu",
        "그리스" : "gr",
        "헝가리" : "hu",
        "중국" : "china",
        "인도" : "india",
        "인도네시아" : "indonesia",
        "대한민국" : "southkorea",
        "일본" : "japan",
        "싱가포르" : "singapore",
        "말레이시아" : "malaysia",
        "홍콩" : "hongkong",
        "태국" : "tahiland",
        "베트남" : "vietnam",
        "필리핀" : "philippines",
        "타이완" : "taiwan",
        "파키스탄" : "pakistan",
        "사우디" : "saudiarabia"
        }
        print(f"""
        name에 넣을 수 있는 국가에 대한 설명은 아래와 같습니다. \n
        {self.country_name}
        """)
        

class BIZIN:
    def __init__(self, name, asia = False):
        self.url = f"https://{name}.bizin.eu/"
        if asia: self.url = f"https://{name}.bizin.asia/"
    
    def DriverSettings(self):
        chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]  #크롬드라이버 버전 확인
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--incognito") # 시크릿 모드
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


    def area(self):
        self.driver.get(self.url)
        self.areas_lst = {}
        self.areas_href = {}
        areas = self.driver.find_element(By.XPATH, '//*[@id="main_categories_dk"]')
        for area in areas.find_elements(By.TAG_NAME, "div"):
            try:
                name = area.text
                pattern = r'\d+'
                numbers = re.findall(pattern, name)
                numbers = [int(n) for n in numbers][0]
                replace_pattern = r'\s*\(\d+\)'
                replace_name = re.sub(replace_pattern, '', name)
                cnt = int(numbers) 
                self.areas_lst[replace_name] = cnt
                href = area.find_element(By.TAG_NAME, "a").get_attribute("href")
                self.areas_href[replace_name] = href
            except:
                print(f"fail {area.text}")
                pass
        self.max_area = max(self.areas_lst, key=self.areas_lst.get)


    def collect(self):
        first = 0
        for num in range(1, self.areas_lst[self.max_area]//20):
            self.driver.get(f"{self.areas_href[self.max_area]}?p={num}")

            try: org_lst = self.driver.find_element(By.XPATH, "/html/body/div/main/div/div[6]/div[1]") 
            except: 
                try: org_lst = self.driver.find_element(By.XPATH, "/html/body/div[2]/main/div/div[6]/div[1]") 
                except: org_lst = self.driver.find_element(By.XPATH, "/html/body/div[3]/main/div/div[6]/div[1]")
            self.href_lst = []
            for href in org_lst.find_elements(By.TAG_NAME, "div"):
                try: self.href_lst.append(href.find_element(By.TAG_NAME, "a").get_attribute("href"))
                except: pass
            self.href_lst = [i for i in self.href_lst if "bizin" in i]
            self.href_lst = list(set(self.href_lst))
            for idx in tqdm(self.href_lst):
                try:
                    self.driver.get(idx)
                    page = self.driver.page_source
                    soup = BeautifulSoup(page, 'html.parser')
                    if first == 0:
                        self.df = pd.read_html(str(soup.find('table')))[0]
                        columns = self.df.transpose().iloc[0]
                        self.df = self.df.T
                        self.df.columns = columns
                        self.df = self.df.drop(0, axis=0)
                        first += 1
                    else:
                        self.append_df = pd.read_html(str(soup.find('table')))[0]
                        self.append_df_columns = self.append_df.transpose().iloc[0]
                        self.append_df = self.append_df.T
                        self.append_df.columns = list(self.append_df_columns)
                        self.append_df = self.append_df.drop(0, axis=0)
                        self.df = self.df.append(self.append_df)
                except:
                    print(C)
