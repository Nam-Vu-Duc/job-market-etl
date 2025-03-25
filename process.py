import psycopg2
import pandas as pd

def process(data) -> None:
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

def store_data():
    conn = psycopg2.connect('host=localhost dbname=jobs user=postgres password=postgres')
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            position VARCHAR(255),
            company VARCHAR(255),
            salary VARCHAR(255),
            address VARCHAR(255),
            exp VARCHAR(255),
        )
        """
    )
    conn.commit()