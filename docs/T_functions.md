# Library Documentation

The library includes the Transform (T) step in the ETL process. 

## Checker Class

The Checker class is used to verify data from a database. When instantiating the class, provide the PostgreSQL database credentials (id, pw, ip, db) and the name of the table to extract data from.

### Methods

#### `read_excel()`
Loads an .xlsx file and converts it into a DataFrame.

#### `read_csv()`
Loads a .csv file and converts it into a DataFrame.

#### `data_update()`
Updates new data by specifying 'I' (insert) or 'U' (update).

#### `date_update()`
Updates the date of new data.

#### `CheckDate()`
Standardizes the date format for general data from Investing.com.

#### `CheckLength()`
Checks the size of the data and trims it to the specified length.

#### `CheckVarchar()`
Checks the size of financial data and inserts a new value if it exceeds the limit.

#### `CheckNumeric()`
Validates if financial data is numeric.

## Analysis Class

The Analysis class is used to analyze data from a database. When instantiating the class, provide the PostgreSQL database credentials (id, pw, ip, db) and the name of the table to analyze data from.

### Methods

#### `read_excel()`
Loads an .xlsx file and converts it into a DataFrame.

#### `read_csv()`
Loads a .csv file and converts it into a DataFrame.

#### `Fail()`
Converts failed data into a dictionary and adds it to a DataFrame.

#### `CheckDate_Duplicate()`
Checks for date and duplicate entries.

#### `CheckNumber()`
Validates if a phone number is valid.
