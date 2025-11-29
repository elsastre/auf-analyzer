from setuptools import setup, find_packages

setup(
    name="webscraper_futbol",
    version="0.1.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "beautifulsoup4",
        "html5lib",
    ],
    entry_points={
        "console_scripts": [
            "webscraper-futbol=webscraper_futbol.__main__:main",
        ]
    },
    python_requires=">=3.8",
)