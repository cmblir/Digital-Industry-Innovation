# Download market data from various information sites

<table border=1 cellpadding=10><tr><td>

*** Important Legal Disclaimer ***
---
Please note that dinnovation is not affiliated, endorsed, or vetted by any source sites. Use at your own risk and discretion.

**For more information about the rights to use the actual data you downloaded, see the Terms of Use for each site. dinnovation is for personal use only.**

</td></tr></table>

---

<a target="new" href="https://pypi.python.org/pypi/dinnovation"><img border=0 src="https://img.shields.io/badge/python-3.9+-blue.svg?style=flat" alt="Python version"></a>
<a target="new" href="https://pypi.python.org/pypi/dinnovation"><img border=0 src="https://img.shields.io/pypi/v/yfinance.svg?maxAge=60%" alt="PyPi version"></a>
<a target="new" href="https://pypi.python.org/pypi/dinnovation"><img border=0 src="https://img.shields.io/pypi/status/yfinance.svg?maxAge=60" alt="PyPi status"></a>


---

## Digital Industry Innovation Data Platform Big data collection and processing, database loading, distribution

It was developed to facilitate the work of collecting, processing, and loading the data required for the Big Data Center.
In addition, various libraries are used in the project, which are available under the Apache 2.0 license.

## Requirements

**required python version**

```Python >= 3.9```

To install the related library, use the command below.
``` pip install requirements.txt ```
or
``` python setup.py install ```

To install the related libray
``` pip install dinnnovation ```

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
cryptography==41.0.3
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
          * ```from dinnovation.collection.financial import investing``` 
          * you can get library infromation
          <pre>
          <code>
          information = investing.information()
          print(information)
          
          """
          The library is divided into two parts. 
          Investing_Crawler, a library that collects data, Investing_Cleanse, a library that processes data
          -------------------------------------------------- 
          The function of Investing_Crawler is shown below. 
          DriverSettings() is a Selenium Chrome driver settings function. 
          download_historial() is a function that collects past stock price data. 
          collect() is a function that collects data from investing.com. 
          -------------------------------------------------- 
          Investing_Cleanse will proceed as soon as you run the class. 
          ------------------------------------------------------------------------------
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
          country_lst = ["japan", "hong-kong"]
          for country in country_lst:
               investing.DriverSettings()
               investing.collect(country, country, f"{country}.xlsx")
          </code>
          </pre>

     * Financial Modeling Prep
          * importing library
          * ```from dinnovation.collection.financial import fmp``` 
          * you can get library infromation
          <pre>
          <code>
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
          fmp = fmp.fmp_extract()
          country_lst = ["호주", "스위스"]

          for country in country_lst:
               fmp.get_symbols(country)
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          fmp = FMP.fmp_extract()
          clean = fmp.cleanse("/", "/")
          </code>
          </pre>

     * Dart (Republic of Korea Only)
          * importing library
          * ```from dinnovation.collection.financial import dart``` 
          * you can get library infromation
          <pre>
          <code>
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
          dart = dart.dart_extract("/.xlsx")         
          extract_finstate = dart.load_finstats('your api key')
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          empty
          </code>
          </pre>

     * idx (Indonesia Only)
          * importing library
          * ```from dinnovation.collection.financial import idx``` 
          * you can get library infromation
          <pre>
          <code>
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
          <pre>
          <code>
          idx_dataframe = pd.read_excel("idx excel path")
          idx = idx.idx_extract(idx_dataframe)

          idx.MakeAvaible()
          
          # add mapping excel file path
          idx.Add_On("mapping_path")
          
          # add idx files path
          idx.transform("files path")

          </code>
          </pre>

     * wsj (USA OTC)
          * importing library
          * ```from dinnovation.collection.financial import wsj``` 
          * you can get library infromation
          <pre>
          <code>
          information = wsj.information()
          print(information)
          """
          The function is described below. 
          The main class in the library is extract().
          extract() collects data from wsj.
          collect() imports only OTC companies of the data collected.
          """
          </code>
          </pre>
          * you can use collecting idx financial information data
          * Example code
          <pre>
          <code>
          wsj = wsj.wsj()

          # US index data is required unconditionally.
          wsj.collect()
          </code>
          </pre>

    
* Company general information data
     * opencorporates
          * importing library
          * ```from dinnovation.collection.general import opencorporates``` 
          * you can get library infromation
          <pre>
          <code>
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
          crawler = opencorporates.opencorporates_extract()
          crawler.Login()
          df = pd.read_excel("finished_url_opencorporates.xlsx")
          for name, url in tqdm(zip(df["country"], df["url"])):
               try: Crawler.GetInformation(url, name)
               except: pass
               Crawler.GetExcel()
          </code>
          </pre>

     * yellow
          * importing library
          * ```from dinnovation.collection.general import yellow``` 
          * you can get library infromation
          <pre>
          <code>
          information = yellow.information()
          print(information)
          """
          The function is described below. \n
          The main class in the library is opencorporates_extract.
          DriverSettings() is a function that sets the driver.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          <code>
          yellow = yellow.yellow_extract()
          yellow.DriverSettings()
          yellow.extract()
          </code>
          </pre>

     * bizin
          * importing library
          * ```from dinnovation.collection.general import bizin``` 
          * you can get library infromation
          <pre>
          <code>
          information = bizin.information()
          print(information)
          """
          A description of the function is given below. 
          The main class within the library is BIZIN. 
          In the case of Asian countries, the url is different, so you need to set it. 
          DriverSettings() is a Selenium Chrome driver settings function. 
          area() is a function that collects information on companies in the country. 
          collect() is a function that collects data from the BIZIN site. 
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          bizin.DriverSettings()
          bizin.area()
          bizin.collect()
          </code>
          </pre>

     * datos (Columbia Only)
          * importing library
          * ```from dinnovation.collection.general import datos``` 
          * you can get library infromation
          <pre>
          <code>
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
          <pre>
          <code>
          # Datos data is required unconditionally.
          datos.make("datos.csv")
          datos.load()
          </code>
          </pre>

     * kemenperin (Italy Only)
          * importing library
          * ```from dinnovation.collection.general import kemenperin``` 
          * you can get library infromation
          <pre>
          <code>
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
          <pre>
          <code>
          kemenperin.DriverSettings()
          kemenperin.get_data()
          kemenperin.load()
          </code>
          </pre>

     * cybo (Ukraina Only)
          * importing library
          * ```from dinnovation.collection.general import cybo``` 
          * you can get library infromation
          <pre>
          <code>
          information = cybo.information()
          print(information)
          """
          The function is described below. \n
          The main class in the library is cybo_extract. \n
          DriverSettings() is a function that sets the driver. \n
          collect() is collect data.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          cybo.DriverSettings()
          cybo.collect()
          </code>
          </pre>

* Company road view picture information data
     * google
          * importing library
          * ```from dinnovation.collection.map import google``` 
          * you can get library infromation
          <pre>
          <code>
          information = google.information()
          print(information)
          """
          A description of the function is given below.
          The main class in the library is map.
          GetStreet() is a function that calls the Google map api.
          collect() is a function that extracts data.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          <code>
          google = google.map("your api key")
          google.GetStrret("address", "/")
          
          # if you need many address pics
          address_info_lst = ["1", "2"]
          google.collect(address_info_lst, "/")
          </code>
          </pre>

* Company stock data
     * marcap
          * importing library
          * ```from dinnovation.collection.stock import marcap``` 
          * you can get library infromation
          <pre>
          <code>
          information = marcap.information()
          print(information)
          """
          A description of the function is given below.
          The main class within the library is MARCAP.
          install() is a function that informs the marcap data github address.
          collect() is a function that extracts data.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          <code>
          ticker_lst = ["1", "2"]
          marcap = marcap.MARCAP(ticker_lst)

          marcap.install()
          marcap.collect()
          </code>
          </pre>

     * shareoutstanding
          * importing library
          * ```from dinnovation.collection.stock import shareoutstanding``` 
          * you can get library infromation
          <pre>
          <code>
          information = shareoutstanding.information()
          print(information)
          """
          The SHAREOUTSTANDING library collects market cap data. 
          DriverSettings() is a Selenium Chrome driver settings function. 
          get_company() is a function that retrieves a ticker from our US company database and stores its value. 
          collect() is a function that collects data from shareoutstanding sites. 
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          <code>
          shareoutstanding.DriverSettings()
          # to access the database
          # Please enter host ip, database, id, password.
          shareoutstanding.get_company(host, database, user, password)
          shareoutstanding.collect()
          </code>
          </pre>

     * yfinance
          * importing library
          * ```from dinnovation.collection.stock import yfinance``` 
          * you can get library infromation
          <pre>
          <code>
          information = yfinance.information()
          print(information)
          """
          The yfinance library collects market cap data. 
          collect() is a function that collects data from shareoutstanding sites.
          """
          </code>
          </pre>
          * you can use collecting opencorporates general information data
          * Example code
          <pre>
          <code>
          # to access the database
          # Please enter host ip, database, id, password.
          yfinance.get_company(host, database, user, password)
          yfinance.collect()
          </code>
          </pre>


### Data Processing

* importing library
     * ```from dinnovation.processing import extract``` 
* you can get library information
<pre>
<code>
information = extract.information()
print(information)
"""
A description of the function is given below. \n
The main class within the library is DataExtract. \n
Enter database id, pw, port, database, table_name in order to connect.\n
The connect() function is a function that tries to connect.\n
The extract() function extracts the database after connecting.
"""
</code>
</pre>
* Data Extract to Database
<pre>
<code>
extract = extract.DataExtract("id", "password", "ip address", "port number", "database name", "table_name")
extract.connect()
extract.extract()
</code>
</pre>

### Data Transformation

* importing library
     * ```from dinnovation.processing import transform``` 
* you can get library information
<pre>
<code>
information = transform.information()
print(information)
"""
A description of the function is given below. 
The library includes T (Transform) in the ETL process. 
The class that checks data in the database is Checker. 
When designating a class, enter the id, pw, ip, db of the database (postgresql), and the table name to be extracted.
The read_excel() function loads xlsx and saves it as a data frame. 
The read_csv() function loads a csv and saves it as a data frame. 
The data_update() function inputs I or U when updating new data. 
The date_update() function inputs the date when new data is updated. 
The CheckDate() function is a function that standardizes the general data date of Investing.com 
The CheckLength() function checks the size of data and cuts it by the size 
The CheckVarchar() function checks the financial data size and inserts a new one if it is large 
The CheckNumeric() function checks a number in financial data 
-------------------------------------------------- ----------------
The class that checks data from database is Analysis. 
The read_excel() function loads xlsx and saves it as a data frame. 
The read_csv() function loads a csv and saves it as a data frame. 
The Fail() function is a function that dictates erroneous data to put into a data frame 
The CheckDate_Duplicate() function is a function that checks the date check and duplicate check 
The CheckNumber() function checks whether a phone number is valid 
"""
</code>
</pre>
* Data Transform
<pre>
<code>
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
     * ```from dinnovation.processing import load```  
* you can get library information
<pre>
<code>
information = load.information()
print(information)
"""
The DataLoad() class is the main one. 
The class can handle large amounts if many = True is set. 
DataLoading() is a function that saves data in the form of a data frame within a class. 
CheckLength() is a function that measures the length of the saved data frame to prevent errors beyond the standard. In addition, the value of keyval is raised above the latest value that currently exists. 
Load() loads the data using a batch process. 
Login() is a function that connects to the database. 
Connect_DB() is a function that connects to the database and creates an environment where data can be loaded.
"""
</code>
</pre>
* Data load to Database
<pre>
<code>
load = load.DataLoad()
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
