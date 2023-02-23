import io
from setuptools import find_packages, setup

setup(
    name="BDC",
    version="1.0.0",
    author="cmblir",
    author_email="sodlalwl13@gmail.com",
    description="Digital Industry Innovation Data Platform Big data collection and processing, database loading, distribution",
    keywords=['Big Data', 'NIA', 'ETL'],
    install_requires=[
        'pandas==1.5.3',
        'numpy==1.24.2',
        'tqdm==4.64.1',
        'OpenDartReader==0.2.1',
        'beautifulsoup4==4.11.2',
        'urllib3==1.26.14',
        'selenium==4.8.2',
        'webdriver_manager==3.8.5',
        'chromedriver_autoinstaller==0.4.0',
        'psycopg2==2.9.5',
        'sqlalchemy==2.0.4',
    ],
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Apache License",
        "Operation System :: OS Independent",
    ],
    python_requires='>=3.9',
)