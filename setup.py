import io
from setuptools import find_packages, setup
from os import path


# --- get version ---
version = "unknown"
with open("dinnovation/version.py") as f:
    line = f.read().strip()
    version = line.replace("version = ", "").replace("'", "")
    
# --- /get version ---

# --- get requirements ---
requirements = None
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with io.open(path.join(here, 'README.md'), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dinnovation",
    version=version,
    author="cmblir",
    author_email="sodlalwl13@gmail.com",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    description="Digital Industry Innovation Data Platform Big data collection and processing, database loading, distribution",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    url = "https://github.com/cmblir/dinnovation",
    keywords=["financial", "data", "investing", "FMP", "idx", "wsj", "dart", "python", "yahoo finance"],
    include_package_data=True,
    packages=find_packages(),
    platforms=['any'],
    python_requires='>=3.9',
    install_requires=requirements
)

print("""
NOTE: dinnovation is not affiliated, endorsed, or vetted by source sites.""")