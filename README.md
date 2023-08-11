## Notice

Please note that DII is not affiliated, endorsed, or vetted by any source sites. Use at your own risk and discretion.


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
          라이브러리는 2개로 나누어집니다.
          데이터를 수집하는 라이브러리인 Investing_Crawler, 데이터를 가공하는 라이브러리인 Investing_Cleanse
          ----------------------------------------------------------------------
          Investing_Crawler의 함수는 아래와 같습니다.
          DriverSettings()은 셀레니움 크롬 드라이버 세팅 함수입니다.
          download_historial()은 과거 주종가 데이터를 수집하는 함수입니다. 
          collect()은 인베스팅 닷컴에서 데이터를 수집하는 함수입니다.
          ------------------------------------------------------------------------------------------
          Investing_Cleanse는 클래스를 실행시키면 바로 진행이 됩니다.
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
          
          crawler = investing.collect("korea", "South Korea", "/")
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
          The function is described below.
          The main class in the library is fmp_extact.
          get_jsonparsed_data() is a function that parses data.
          Extractor() is a function that imports data in json form.
          url_generator() is a function of accessing the FMP site and isolating the data.
          ending_period_extact() is a function that standardizes dates.
          report_type_extract() is a function that distinguishes between annual and quarterly based on incoming values.
          GetExcel() is a function that stores the extracted data.
          cleanse() is a function that processes data.
          get_symbols() is a function that imports data from the site.
          Make_clean() is a function that executes the above functions sequentially to extract and store data.
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
          The function is described below.
          The main class in the library is dart_extract.
          api_key() is a function that tells the api key.
          Extract_finstate() is a function that extracts data.
          load_finstate() is a function that stores data.
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
          The function is described below.
          The main class in the library is idx_extact.
          make_Available() is a function that enables data frames.
          Add_On() is a function that creates data.
          Transform() is a function that processes data.
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
          The function is described below.
          The main class in the library is opencorporates_extract.
          DriverSettings() is a function that sets the driver.
          Login() is a function to log in to the opensporates.
          ReCounty() is a function that selects a country.
          SearchCompanies() is a function that finds a company.
          GetInformation() is a function that extracts data.
          GetExcel() is a function that stores the extracted data.
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
          </code>
          </pre>

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
          The function is described below.
          The main class in the library is datos_extact.
          Make() is a function that processes data.
          load() is a function that stores data.
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
          The function is described below.
          The main class in the library is datos_extact.
          DriverSettings() is a function that runs the Chrome driver.
          get_data() is a function that extracts and processes data.
          load() is a function that stores data.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code

### Data Processing

* importing library
     * ```from DataQuality.ETL import EL``` 
* Data Extract to Database

<pre>
<code>
extract = EL.DataExtract()
extract.connect("id", "password", "ip address", "port number", "database name", "table_name")
extract.extract()
</code>
</pre>

### Data Transformation

* importing library
     * ```from DataQuality.ETL import T``` 
* Data Extract to Database

<pre>
<code>
import pandas as pd

transform = T.Checker()
# if you data type is xlsx 
transform.read_excel("path")
# if you data type is csv
transform.read_csv("path")
"""
func is many options.
1. if you need data normalization fndtn_dt, you can use transform.fndtn_dt()
2. if you need insert data update information, you can use transform.data_update() or update is transform.data_update(Insert = False)
3. if you need data check date, you can use transform.CheckDate()
4. if you need data check length, you can use transform.CheckLength()
5. if you need data check numeric type, you can use transform.CheckNumeric()
6. if you need data check varchar type, you can use transform.CheckVarchar()
"""
transform.df.to_excel("~.xlsx")
</code>
</pre>


### Data Load

* importing library
     * ```from DataQuality.ETL import EL``` 
* Data Extract to Database
<pre>
<code>
load = EL.DataLoad()
# if you loading data is many, many argument is True
load.Login("user", "password", "host", "port", "dbname")
load.DataLoading("path")
"""
func is options.
1. if you need data check length you can use load.CheckLength()
"""
load.Connect_DB()
# if you need replace data, you can use argument load.Connect_DB(replace = True)
# if you loading a data is first time, you can use argument load.Connect_DB(first = False)
load.Load()
</code>
</pre>

### Data Analysis
