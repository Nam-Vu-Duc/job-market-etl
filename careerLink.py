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
        ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.list-group.mt-4'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.media-body.overflow-hidden")

    data = []
    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h5.job-name").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "a.text-dark").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "span.job-salary").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_element(By.CSS_SELECTOR, "div.job-location div a").text.strip()
        except NoSuchElementException:
            address = "Not Available"

        data.append([position, company, salary, address])

    return data

def get_total_pages() -> int:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://www.careerlink.vn/viec-lam/k/data-engineer?page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.pagination'))
        )

    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'li')
        total_page = int(li_list[-2].text)
        return total_page

    driver.quit()

    return 1

def get_all_jobs() -> list[list[str]]:
    data = []
    total_page = get_total_pages()

    for i in range(1,total_page+1):
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://www.careerlink.vn/viec-lam/k/data-engineer?page={i}')
        data += get_job_list(driver)
        driver.quit()

    return data

# if __name__ == '__main__':
#     data = get_all_jobs()
#     data = pd.DataFrame(data)
#     data.columns = ['Vị trí', 'Công ty', 'Mức lương', 'Địa chỉ']
#     print(data)