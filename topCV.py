from pandas import DataFrame
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

def get_total_pages() -> int:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://www.topcv.vn/tim-viec-lam-data-engineer?type_keyword=1&page=1')

    pagination = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.ID, 'job-listing-paginate-text'))
    )

    total_page = int(pagination.text.split()[2])

    driver.quit()

    return total_page

def get_all_jobs() -> DataFrame:
    data = []
    total_page = get_total_pages()

    for i in range(1,total_page+1):
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://www.topcv.vn/tim-viec-lam-data-engineer?type_keyword=1&page={i}')
        data += get_job_list(driver)
        driver.quit()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    data = pd.DataFrame(data)
    data.columns = ['position', 'company', 'salary', 'address', 'exp']

    data['source'] = 'topCV'

    # Ensure salary column is a string and stripped of leading/trailing spaces
    data['salary'] = data['salary'].astype(str).str.strip()

    # Initialize columns
    data['min_salary'] = 0
    data['max_salary'] = 0

    # Case 1: 'Thoả thuận' → No salary values
    condition = data['salary'].str.contains('Thoả thuận', na=False)
    data.loc[condition, ['min_salary', 'max_salary']] = [0, 0]

    # Case 2: 'Tới X USD' or 'Tới X triệu'
    condition = data['salary'].str.startswith('Tới')
    is_usd = data['salary'].str.contains('USD', na=False)
    data.loc[condition, 'max_salary'] = data.loc[condition, 'salary'].str.split().str[1].str.replace(',', '').astype(float)
    data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
    data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

    # Case 3: 'X - Y USD' or 'X - Y triệu'
    condition = data['salary'].str.contains('-')
    split_salaries = data.loc[condition, 'salary'].str.replace(',', '').str.split(' - ')

    data.loc[condition, 'min_salary'] = split_salaries.str[0].astype(float)
    data.loc[condition, 'max_salary'] = split_salaries.str[1].str.split().str[0].astype(float)
    data.loc[condition & is_usd, ['min_salary', 'max_salary']] *= 25000 / 1000000

    print(data)

    return data

get_all_jobs()