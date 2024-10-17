import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import logging
import psycopg2
import psycopg2.extras

load_dotenv('.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

hostname = os.getenv('PG_HOST')
database = os.getenv('PG_DATABASE')
username = os.getenv('PG_USER')
pwd = os.getenv('PG_PASSWORD')
port_id = 5432

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('AWS_BUCKET_NAME')

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def fetch_and_clean_s3_data():
    """
    Fetch CSV files from S3 for the previous day, combine them into a single DataFrame,
    and clean the data by deduplicating and standardizing columns.

    This function performs the following tasks:
    1. Retrieves CSV files from S3 based on the date.
    2. Combines the CSV files into a single DataFrame.
    3. Deduplicates the data by the 'job_key' column.
    4. Standardizes the column names by converting to lowercase and replacing spaces with underscores.
    5. Adds 'update_timestamp' columns and edit 'pub_date' with data formatting.

    Returns:
        pandas.DataFrame: The cleaned and combined data.
    """
    eastern = pytz.timezone('America/New_York')
    yesterday_date = (datetime.now(eastern) - timedelta(days=1)).strftime('%Y-%m-%d')
    logging.info(f"Fetching files for date: {yesterday_date}")
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=yesterday_date)
    combined_df = pd.DataFrame()

    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            if 'indeed' in file_key and file_key.endswith('.csv'):
                print(f"Downloading {file_key} from S3...")
                obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                csv_data = obj['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_data))
                combined_df = pd.concat([combined_df, df], ignore_index=True)
    else:
        logging.warning(f"No files found for date {yesterday_date}.")

    logging.info(f"Total rows combined: {len(combined_df)}")

    if not combined_df.empty:
        logging.info("Starting data cleaning process...")
        deduplicated_df = combined_df.drop_duplicates(subset=['Job Key'], keep='first')
        deduplicated_df.columns = deduplicated_df.columns.str.lower().str.replace(' ', '_')
        deduplicated_df['pub_date'] = pd.to_datetime(deduplicated_df['pub_date'], unit='ms').dt.date
        deduplicated_df['update_timestamp'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        deduplicated_df['update_timestamp'] = deduplicated_df['update_timestamp'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        logging.info("Data cleaning complete.")
        return deduplicated_df
    else:
        logging.warning("No data to clean.")
        return pd.DataFrame()

def get_value(value):
    """Return None for NaN values, otherwise return the value."""
    return value if pd.notnull(value) else None

def create_table():
    """Create the raw_job_data table in the PostgreSQL database."""
    try:
        with psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        ) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute('DROP TABLE IF EXISTS job_market_raw.raw_job_data')
                create_script = ''' 
                CREATE TABLE IF NOT EXISTS job_market_raw.raw_job_data (
                    job_key              varchar(255) PRIMARY KEY,
                    feed_id              int,
                    company_search       varchar(255),
                    companyoverviewlink  varchar(255),
                    rating_search        float,
                    review_count_search  int,
                    title_search         varchar(255),
                    salary_max           float,
                    salary_min           float,
                    salary_type          varchar(50),
                    location_search      varchar(255),
                    relative_time        varchar(50),
                    city                 varchar(100),
                    city_extras          varchar(100),
                    postal               varchar(20),
                    state                varchar(50),
                    pub_date             date,
                    currency             varchar(20),
                    salary_info          text,
                    taxonomyattributes   text,
                    job_type_search      varchar(100),
                    link                 text,
                    company_job          varchar(255),
                    overview_link        varchar(255),
                    review_link          text,
                    rating_job           float,
                    review_count_job     int,
                    title_job            varchar(255),
                    subtitle             varchar(255),
                    location_job         varchar(255),
                    job_type_job         varchar(100),
                    job_description      text,
                    update_timestamp     timestamp
                ) '''
                cur.execute(create_script)
                conn.commit()
                logging.info("Table created successfully!")

    except Exception as error:
        logging.error(f"Error creating table: {error}")

def insert_data(deduplicated_df):
    """Insert data into the raw_job_data table."""
    try:
        with psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        ) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                insert_script = '''
                    INSERT INTO job_market_raw.raw_job_data (
                        job_key, feed_id, company_search, companyoverviewlink, rating_search,
                        review_count_search, title_search, salary_max, salary_min, salary_type,
                        location_search, relative_time, city, city_extras, postal,
                        state, pub_date, currency, salary_info, taxonomyattributes,
                        job_type_search, link, company_job, overview_link, review_link,
                        rating_job, review_count_job, title_job, subtitle, location_job,
                        job_type_job, job_description, update_timestamp
                    ) VALUES %s
                    ON CONFLICT (job_key) DO NOTHING;
                '''
                insert_values = [
                    (
                        row.job_key,
                        get_value(row.feed_id),
                        get_value(row.company_search),
                        get_value(row.companyoverviewlink),
                        get_value(row.rating_search),
                        get_value(row.review_count_search),
                        get_value(row.title_search),
                        get_value(row.salary_max),
                        get_value(row.salary_min),
                        get_value(row.salary_type),
                        get_value(row.location_search),
                        get_value(row.relative_time),
                        get_value(row.city),
                        get_value(row.city_extras),
                        get_value(row.postal),
                        get_value(row.state),
                        pd.to_datetime(row.pub_date, errors='coerce'),
                        get_value(row.currency),
                        get_value(row.salary_info),
                        get_value(row.taxonomyattributes),
                        get_value(row.job_type_search),
                        get_value(row.link),
                        get_value(row.company_job),
                        get_value(row.overview_link),
                        get_value(row.review_link),
                        get_value(row.rating_job),
                        get_value(row.review_count_job),
                        get_value(row.title_job),
                        get_value(row.subtitle),
                        get_value(row.location_job),
                        get_value(row.job_type_job),
                        get_value(row.job_description),
                        pd.to_datetime(row.update_timestamp, errors='coerce') 
                    )
                    for row in deduplicated_df.itertuples(index=False)
                ]
                psycopg2.extras.execute_values(cur, insert_script, insert_values)
                conn.commit()
                logging.info("Data inserted successfully!")

    except Exception as error:
        logging.error(f"Error inserting data: {error}")
