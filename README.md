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
## Dinnovation Architecture

![dinnovation_module](https://github.com/cmblir/dinnovation/assets/75519839/9349976d-8774-4831-b17e-716d9a3f3498)
- Dinnovation's architecture is built on efficiency. Data from multiple sites can be collected, processed, and loaded with one module.

## Dinnovation Process

![auto_process](https://github.com/cmblir/dinnovation/assets/75519839/b0fba513-de33-49b2-96a6-b182ca5965d7)
- Dinnovation's Auto Process automatically collects, processes, and loads data using the scheduler. This can be done more efficiently than simply using a module.


## Dinnovation Workflow

![github_workflow](https://github.com/cmblir/dinnovation/assets/75519839/462facc4-8926-481b-8963-8c55f7941ddb)
- Dinnovation's workflow was created by a single developer. This will automatically update the package and the release of Github when you modify the code and upload it to Git for the efficiency of your work. In addition, version management is also automatically.

## How to use

[Guide](./quick_start/README.md)