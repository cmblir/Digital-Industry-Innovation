# Digital Industry Innovation Data Platform Big data collection and processing, database loading, distribution

It was developed to facilitate the work of collecting, processing, and loading the data required for the Big Data Center.
In addition, various libraries are used in the project, which are available under the Apache 2.0 license.

## Requirements

**required python version**

```Python >= 3.9```

To install the related library, use the command below.
``` pip install requirements.txt ```
or
``` python setup.py install ```

**required library**

```
pandas==1.5.3
numpy==1.24.2
tqdm==4.64.1
OpenDartReader==0.2.1
beautifulsoup4==4.11.2
urllib3==1.26.14
selenium==4.8.2
webdriver_manager==3.8.5
chromedriver_autoinstaller==0.4.0
psycopg2==2.9.5
sqlalchemy==2.0.4
```

---
## How to use

### Data collection

- Data collection is currently divided into three categories.
* Corporate financial information data
* Company general information data
* Company valuation data

- The sites used for collection are as follows.

* Corporate financial information data
     * Investing
          * importing library
          * ```from DataColletion.Collection.Financial import INVESTING``` 
          * you can get library infromation
          <pre>
          <code>
          investing = INVESTING
          information = investing.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>
          * you can use collecting investing financial information data
          * Example code
          <pre>
          <code>
          investing = INVESTING.Investing_Crawler("/~.xlsx")
          # An argument is the material path that contains the content to be matched.
          
          settings = investing.DriverSettings()
          # if you want use Turn off Warning, use argument Turn_off_warning = True
          # if you want use Linux mode on Background, use argument linux_mode = True
          
          crawler = investing.investingDotcom_crawler("korea", "South Korea", "/")
          # if you want crawlering Singapore, use argument isSingapore = True          
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          investing = INVESTING.Investing_Cleanse("/~.xlsx", "/~.xlsx")
          process = investing.matching_process()
          </code>
          </pre>

     * Financial Modeling Prep
          * importing library
          * ```from DataColletion.Collection.Financial import FMP``` 
          * you can get library infromation
          <pre>
          <code>
          fmp = FMP
          information = fmp.information()
          print(information)
          
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 fmp_extact입니다. \n
          get_jsonparsed_data()는 데이터를 파싱하는 함수입니다. \n
          extractor()은 데이터를 json형태로 가지고 오는 함수입니다. \n
          url_generator()은 FMP 사이트에 접속하여 데이터를 분리하는 함수입니다. \n
          ending_period_extact()는 날짜를 표준화하는 함수입니다. \n
          report_type_extract()는 들어오는 값에 따라 연간인지 분기인지 구분하는 함수입니다. \n
          GetExcel()은 추출한 데이터를 저장하는 함수입니다. \n
          cleanse()는 데이터를 가공하는 함수입니다. \n
          get_symbols()는 데이터를 사이트로부터 가져오는 함수입니다. \n
          make_clean()은 위의 함수들을 순차적으로 실행하여 데이터를 추출 후 저장하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting Financial Modeling Prep data
          * Example code
          <pre>
          <code>
          fmp = FMP.fmp_extract()
          get_symbols = fmp.get_symbols("한국")
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          fmp = FMP.fmp_extract()
          clean = fmp.make_clean("/", "/")
          </code>
          </pre>

     * Dart (Republic of Korea Only)
          * importing library
          * ```from DataColletion.Collection.Financial import DART``` 
          * you can get library infromation
          <pre>
          <code>
          dart = DART
          information = dart.information()
          print(information)
          
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 dart_extract입니다. \n
          api_key()는 api key를 알려주는 함수입니다. \n
          extract_finstate()은 데이터를 추출하는 함수입니다. \n
          load_finstate()은 데이터를 저장하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting Dart financial information data
          * Example code
          <pre>
          <code>
          dart = DART.dart_extract("/.xlsx")
          print(dart.api_key())
          
          """ print is api_key1 = 'example key code 0000' """
          
          extract_finstate = dart.load_finstats('example key code 0000')
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          empty
          </code>
          </pre>

     * Vietstock (Viet Nam Only)
          * importing library
          * ```from DataColletion.Collection.Financial import VIETSTOCK``` 
          * you can get library infromation
          <pre>
          <code>
          vietstock = VIETSTOCK
          information = vietstock.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>
          * you can use collecting Vietstock financial information data
          * Example code

     * idx (Indonesia Only)
          * importing library
          * ```from DataColletion.Collection.Financial import IDX``` 
          * you can get library infromation
          <pre>
          <code>
          idx = IDX
          information = idx.information()
          print(information)
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 idx_extact입니다. \n
          make_Avaible()는 데이터프레임을 사용할 수 있게하는 함수입니다. \n
          Add_On()은 데이터를 만드는 함수입니다. \n
          transform()은 데이터를 가공하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting idx financial information data
          * Example code
    
* Company general information data
     * opencorporates
          * importing library
          * ```from DataColletion.Collection.General import OPENCORPORATES``` 
          * you can get library infromation
          <pre>
          <code>
          opencorporates = OPENCORPORATES
          information = opencorporates.information()
          print(information)
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 opencorporates_extract입니다. \n
          DriverSettings()는 드라이버 세팅을 하는 함수입니다. \n
          Login()은 opencorporates에 로그인하는 함수입니다. \n
          ReCounty()는 국가를 선택하는 함수입니다. \n
          SearchCompanies()는 기업을 찾는 함수입니다. \n
          GetInformation()은 데이터를 추출하는 함수입니다. \n
          GetExcel()은 추출한 데이터를 저장하는 함수입니다. \n
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          <code>
          opencorporates = OPENCORPORATES
          crawler = opencorporates.opencorporates_extract()
          crawler.Login()
          df = pd.read_excel("finished_url_opencorporates.xlsx")
          for name, url in tqdm(zip(df["country"], df["url"])):
               try: Crawler.GetInformation(url, name)
               except: pass
               Crawler.GetExcel()
        </pre>
        </code>

     * datos (Columbia Only)
          * importing library
          * ```from DataColletion.Collection.General import DATOS``` 
          * you can get library infromation
          <pre>
          <code>
          datos = DATOS
          information = datos.information()
          print(information)
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 datos_extact입니다. \n
          make()는 데이터를 가공하는 함수입니다. \n
          load()은 데이터를 저장하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code

     * ESERCIZI (Indonesia Only)
          * importing library
          * ```from DataColletion.Collection.General import ESERCIZI``` 
          * you can get library infromation
          <pre>
          <code>
          esercizi = ESERSIZI
          information = esercizi.information()
          print(information)
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 datos_extact입니다. \n
          make()는 데이터를 가공하는 함수입니다. \n
          load()은 데이터를 저장하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code

     * kemenperin (Italy Only)
          * importing library
          * ```from DataColletion.Collection.General import KEMENPERIN``` 
          * you can get library infromation
          <pre>
          <code>
          kemenperin = KEMENPERIN
          information = kemenperin.information()
          print(information)
          """
          함수에 대한 설명은 아래와 같습니다. \n
          라이브러리 내 주요 클래스는 datos_extact입니다. \n
          DriverSettings()는 크롬 드라이버를 실행하는 함수입니다. \n
          get_data()는 데이터를 추출하여 가공하는 함수입니다. \n
          load()은 데이터를 저장하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code

### Data Processing


### Loading Data