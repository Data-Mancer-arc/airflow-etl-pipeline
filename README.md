# airflow-etl-pipeline

Airflow ETL Pipeline: Stock Market Data
This project demonstrates building an automated ETL pipeline using Apache Airflow to extract daily stock market data from the Polygon API, transform JSON data into a tabular format with Python and pandas, and load the results into a local SQLite database.

Features
Automated extraction of stock data using the Polygon API

Data transformation and flattening for database storage

SQLite integration with Airflow using TaskFlow API and SqliteHook

Daily scheduling, error handling, and task dependencies in Airflow

Easy extension for additional stock tickers or schedule changes

Setup
Install requirements:
pip install apache-airflow pandas requests

Create Airflow connection for SQLite (market_database_conn)

Add your Polygon API key in the DAG code

Start Airflow and trigger the DAG from the UI or command line

Project Structure
dags/: Contains the Airflow DAG and Python tasks

market_data.db: SQLite database for storing extracted and transformed data

Usage
Run the DAG to fetch, process, and store daily stock data

Query the market_data table in SQLite to access results
