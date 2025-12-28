import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def create_driver():
    headless = os.getenv("SELENIUM_HEADLESS", "1") == "1"
    browser = os.getenv("SELENIUM_BROWSER", "chrome").lower()
    if browser != "chrome":
        raise ValueError("현재 구현은 chrome만 지원합니다.")

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    # Service로 driver path를 넘김
    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver
