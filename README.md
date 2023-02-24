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
          investing = INVESTING()
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
          fmp = FMP()
          information = fmp.information()
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
          fmp = FMP.FMP_Extracter()
          get_symbols = fmp.get_symbols("한국")
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          fmp = FMP.FMP_Extracter()
          clean = fmp.make_clean("/", "/")
          </code>
          </pre>

     * Dart (Republic of Korea Only)
          * importing library
          * ```from DataColletion.Collection.Financial import DART``` 
          * you can get library infromation
          <pre>
          <code>
          dart = DART()
          information = dart.information()
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
          dart = DART.Dart_Extract("/.xlsx")
          print(dart.api_key())
          """ print is api_key1 = 'example key code 0000' """
          extract_finstate = dart.load_finstats('example key code 0000')
          </code>
          </pre>
          * you can use transform data
          * Example code
          <pre>
          <code>
          fmp = FMP.FMP_Extracter()
          clean = fmp.make_clean("/", "/")
          </code>
          </pre>

     * Vietstock (Viet Nam Only)
          * importing library
          * ```from DataColletion.Collection.Financial import VIETSTOCK``` 
          * you can get library infromation
          <pre>
          <code>
          vietstock = VIETSTOCK()
          information = vietstock.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>

     * idx (Indonesia Only)
          * importing library
          * ```from DataColletion.Collection.Financial import IDX``` 
          * you can get library infromation
          <pre>
          <code>
          idx = IDX()
          information = idx.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>

    
* Company general information data
     * opencorporates
          * importing library
          * ```from DataColletion.Collection.General import OPENCORPORATES``` 
          * you can get library infromation
          <pre>
          <code>
          opencorporates = OPENCORPORATES()
          information = opencorporates.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>
     * datos (Columbia Only)
          * importing library
          * ```from DataColletion.Collection.General import DATOS``` 
          * you can get library infromation
          <pre>
          <code>
          datos = DATOS()
          information = datos.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>
     * ESERCIZI (Indonesia Only)
          * importing library
          * ```from DataColletion.Collection.General import ESERCIZI``` 
          * you can get library infromation
          <pre>
          <code>
          esercizi = ESERSIZI()
          information = esercizi.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>
     * kemenperin (Italy Only)
          * importing library
          * ```from DataColletion.Collection.General import KEMENPERIN``` 
          * you can get library infromation
          <pre>
          <code>
          kemenperin = KEMENPERIN()
          information = kemenperin.information()
          print(information)
          """
          print is 
          """
          </code>
          </pre>

### Data Processing


### Loading Data