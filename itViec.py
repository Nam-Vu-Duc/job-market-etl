from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import requests
import pandas as pd
import random
import time
import os

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-blink-features=AutomationControlled")  # Hide Selenium

def get_job_list(driver) -> list[list[str]]:
    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CSS_SELECTOR, 'div.col-xl-5.card-jobs-list.ips-0.ipe-0.ipe-xl-6'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.job-card")

    data = []
    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h3.imt-3").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "span.ims-2.small-text.text-hover-underline a").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "div.d-flex.align-items-center.salary.text-rich-grey a").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_elements(By.CSS_SELECTOR, "span.ips-2.small-text.text-rich-grey")[1].text.strip()
        except NoSuchElementException:
            address = "Not Available"

        data.append([position, company, salary, address])

    return data

def get_total_pages() -> int:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://itviec.com/it-jobs/data-engineer?page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'nav.ipagination.imt-10'))
        )

    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'div.page')
        total_page = int(li_list[-2].text)
        return total_page

    driver.quit()

    return 1

def get_all_jobs() -> list[list[str]]:
    data = []
    total_page = get_total_pages()

    for i in range(1,total_page+1):
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://itviec.com/it-jobs/data-engineer?page={i}')
        data += get_job_list(driver)
        driver.quit()

    return data

# if __name__ == '__main__':
#     data = get_all_jobs()
#     data = pd.DataFrame(data)
#     data.columns = ['Vị trí', 'Công ty', 'Mức lương', 'Địa chỉ']
#     print(data)