import io
from setuptools import find_packages, setup
from os import path


# --- get version ---
version = "unknown"
with open("DII/version.py") as f:
    line = f.read().strip()
    version = line.replace("version = ", "").replace('"', '')
# --- /get version ---

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with io.open(path.join(here, 'README.md'), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="DII",
    version=version,
    author="cmblir",
    author_email="sodlalwl13@gmail.com",
    description="Digital Industry Innovation Data Platform Big data collection and processing, database loading, distribution",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: Apache License",
        "Operation System :: OS Independent",
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=find_packages(),
    platforms=['any'],
    python_requires='>=3.9',
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
    'sqlalchemy==2.0.4']
    
    ,

)

print("""
NOTE: DII is not affiliated, endorsed, or vetted by source sites.""")