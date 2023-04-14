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
          함수에 대한 설명은 아래와 같습니다. 
          라이브러리 내 주요 클래스는 fmp_extact입니다. 
          get_jsonparsed_data()는 데이터를 파싱하는 함수입니다. 
          extractor()은 데이터를 json형태로 가지고 오는 함수입니다. 
          url_generator()은 FMP 사이트에 접속하여 데이터를 분리하는 함수입니다. 
          ending_period_extact()는 날짜를 표준화하는 함수입니다. 
          report_type_extract()는 들어오는 값에 따라 연간인지 분기인지 구분하는 함수입니다. 
          GetExcel()은 추출한 데이터를 저장하는 함수입니다. 
          cleanse()는 데이터를 가공하는 함수입니다. 
          get_symbols()는 데이터를 사이트로부터 가져오는 함수입니다. 
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
          함수에 대한 설명은 아래와 같습니다. 
          라이브러리 내 주요 클래스는 dart_extract입니다. 
          api_key()는 api key를 알려주는 함수입니다. 
          extract_finstate()은 데이터를 추출하는 함수입니다. 
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
          함수에 대한 설명은 아래와 같습니다. 
          라이브러리 내 주요 클래스는 idx_extact입니다. 
          make_Avaible()는 데이터프레임을 사용할 수 있게하는 함수입니다. 
          Add_On()은 데이터를 만드는 함수입니다. 
          transform()은 데이터를 가공하는 함수입니다.
          """
          </code>
          </pre>
          * you can use collecting idx financial information data
          * Example code