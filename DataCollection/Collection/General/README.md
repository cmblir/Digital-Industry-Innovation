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