JOB MARKET ETL
===============================

## I. Introduction
I implemented a pipeline to scrape, process, and analyze the job market by each job's address, location and exp.

## II. System Components

![image](https://github.com/user-attachments/assets/b40bca06-6252-470f-9cdc-6e7398b9322c)

- **initial_requirements.py**: create required tables in mysql and postgres and kafka topics for further workflow
- **scrape_data**: scrape jobs from website using Selenium and clean these data using Pandas, then save cleaned data to mysql and produce to kafka topic 'jobs-topic'
- **process_data**: consume data from topic 'jobs-topic, process these data using spark, then produce to new kafka topics ('address_report', 'location_report', 'exp_report')
- **visualize_report**: Consumes reports from Kafka topics (address_report, location_report, exp_report) and stores them in PostgreSQL for Superset dashboard visualization.
- **send_email**: Fetch data from PostgreSQL and send email to user

## III. Steps to Run
### 1. Clone this repository.

### 2. Config Requirements and Dockerfile
#### Build Image with requirement installation.
```bash
docker build -t airflow-webscraping .
```

#### Build Docker container
```bash
docker-compose up -d
```

### 3. Config Superset Docker:
#### Enter the Superset repository
```bash
cd superset
```

#### Fire up Superset using Docker Compose
```bash
docker compose -f docker-compose-image-tag.yml up
```
### 4. Install the required Python packages using the following command:
```bash
pip install -r requirements.txt
```

### Additional Configuration
If you need to modify configurations or change the exposed port, you can update the `docker-compose.yml` file according to your requirements.

## IV. Running the App
### Airflow
- Open http://localhost:8080 in your browser.
- User/password: airflow / airflow.
- Run dag_for_jobs_market_etl
  
![image](https://github.com/user-attachments/assets/af3ce9b7-e0f7-45b1-a7b7-da3fea78406d)

### MySQL

![image](https://github.com/user-attachments/assets/7d2f844b-6aec-4c59-855b-ff9a57926155)

### Form Email
- After Airflow is done (about 30 minutes), receive email about total jobs and top highest salary jobs
  
![image](https://github.com/user-attachments/assets/8d287130-ef5e-4f6d-95fe-c44ce394b992)

### Superset
- Open http://localhost:8088 in your browser.
- User / password: admin / admin.

![image](https://github.com/user-attachments/assets/f707027e-aea8-429c-8ef5-adc7036bd90d)

![image](https://github.com/user-attachments/assets/ab19eef3-a9ec-4290-b845-c0793b457b3e)

![image](https://github.com/user-attachments/assets/82d840e4-fdb9-462e-ab48-e610d78364e1)




