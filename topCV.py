from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import requests
import pandas as pd
import random
import time
import os

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-blink-features=AutomationControlled")  # Hide Selenium

#  = webdriver.Chrome(service=Service(), options=chrome_options)
#
def get_job_list(driver) -> list[list[str]]:
    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CLASS_NAME, 'job-list-search-result'))
    )

    job_lists = job_lists_container.find_elements(By.CLASS_NAME, 'job-item-search-result')

    data = []
    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h3.title a span").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "a.company span").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "label.title-salary").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_element(By.CSS_SELECTOR, "label.address span").text.strip()
        except NoSuchElementException:
            address = "Not Available"

        try:
            exp = job.find_element(By.CSS_SELECTOR, "label.exp span").text.strip()
        except NoSuchElementException:
            exp = "Not Available"

        data.append([position, company, salary, address, exp])

    return data

def get_all_jobs() -> list[list[str]]:
    data = []
    for i in range(1,3):
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://www.topcv.vn/tim-viec-lam-data-engineer-tai-ha-noi-kl1?type_keyword=1&page={i}&locations=l1&sba=1')
        data += get_job_list(driver)
        driver.quit()

    return data

if __name__ == '__main__':
    data = get_all_jobs()

    data = pd.DataFrame(data)
    data.columns = ['Vị trí', 'Công ty', 'Mức lương', 'Địa chỉ', 'Kinh nghiệm']

