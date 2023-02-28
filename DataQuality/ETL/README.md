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
