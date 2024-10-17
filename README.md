# Build Data Cleaning Task

## **Description**

The **Build Data Cleaning Task** focuses on cleaning and preparing raw job data scraped from the Indeed website for downstream processes. This involves loading data from Amazon S3, performing deduplication, renaming columns for consistency, and adding metadata to facilitate tracking and analysis.

## **Steps**

### 1. **Load Data from S3**
Retrieve the CSV files generated in Task 1 (Job Scraper) from the designated S3 bucket.

![Load Data from S3](https://prod-files-secure.s3.us-west-2.amazonaws.com/fb22abfd-f32d-4f6c-97dc-5f99b88963a7/a2d958c3-1a6e-4f41-ba00-37faceb12fc4/Blank_diagram.png)

### 2. **Combine Job Data Files**
Combine multiple CSV files (from different job titles or sources) into a single dataset.

- **Example Files:**
  - `2024-09-26_Canada_data analyst_indeed.csv`
  - `2024-09-26_Canada_data engineer_indeed.csv`
  - `2024-09-26_Canada_data scientist_indeed.csv`
  - `2024-09-26_Canada_machine learning engineer_indeed.csv`
  - `2024-09-26_US_data analyst_indeed.csv`
  - `2024-09-26_US_data engineer_indeed.csv`
  - `2024-09-26_US_data scientist_indeed.csv`
  - `2024-09-26_US_machine learning engineer_indeed.csv`

- **Reference:** [Combine Data Files](https://drive.google.com/file/d/1-0s67NF0LXkuQm6PSL-qV_xtNcDVQJTq/view?usp=sharing)

### 3. **Deduplicate on Job Key**
Identify and remove duplicate job postings using the unique `job_key` to ensure the dataset contains only distinct job listings.

- **Duplicates Removed:** 14

### 4. **Keep All Columns**
Ensure that all columns from the raw dataset are retained for further analysis.

![Keep All Columns](https://prod-files-secure.s3.us-west-2.amazonaws.com/fb22abfd-f32d-4f6c-97dc-5f99b88963a7/aab28010-b678-4081-9791-907f7f17e0c6/Screenshot_2024-09-28_at_12.56.26_PM.png)

### 5. **Rename Columns**
Rename columns to use lowercase letters and underscores for consistency (e.g., `title_search`).

![Rename Columns](https://prod-files-secure.s3.us-west-2.amazonaws.com/fb22abfd-f32d-4f6c-97dc-5f99b88963a7/a15c5cd0-934d-47dc-a56b-e59cd196b1d2/Screenshot_2024-09-28_at_2.50.31_PM.png)

### 6. **Add Update Timestamp Column (Add Metadata)**
Introduce an `update_timestamp` column to track when each record was last modified or processed.

![Add Update Timestamp](https://prod-files-secure.s3.us-west-2.amazonaws.com/fb22abfd-f32d-4f6c-97dc-5f99b88963a7/cbbc5705-44fd-4544-9b76-174011d73426/Screenshot_2024-09-28_at_3.19.35_PM.png)

### 7. **Create and Import Cleaned Data to PostgreSQL**
Create the `cleaned_job_data_9_26` table in the `job_market_raw` schema and import the cleaned CSV data.

![Import to PostgreSQL](https://prod-files-secure.s3.us-west-2.amazonaws.com/fb22abfd-f32d-4f6c-97dc-5f99b88963a7/c2496fa5-aa2f-46db-a7ce-0264a9b222b1/Screenshot_2024-09-28_at_6.39.03_PM.png)

## **Notes**



- **Step 1 - Load Data from S3**
  - Retrieved the CSV files from the S3 bucket.
  - **Files Retrieved:**
    - `2024-09-26_Canada_data analyst_indeed.csv`
    - `2024-09-26_Canada_data engineer_indeed.csv`
    - `2024-09-26_Canada_data scientist_indeed.csv`
    - `2024-09-26_Canada_machine learning engineer_indeed.csv`
    - `2024-09-26_US_data analyst_indeed.csv`
    - `2024-09-26_US_data engineer_indeed.csv`
    - `2024-09-26_US_data scientist_indeed.csv`
    - `2024-09-26_US_machine learning engineer_indeed.csv`

- **Step 2 - Combine Job Data Files**
  - Combined multiple job data files into a single dataset.
  - **Reference:** [Combine Data Files](https://drive.google.com/file/d/1-0s67NF0LXkuQm6PSL-qV_xtNcDVQJTq/view?usp=sharing)

- **Step 3 - Deduplicate on Job Key**
  - Removed 14 duplicate job postings based on `job_key`.

- **Step 4 - Keep All Columns**
  - Ensured all original columns were retained.

- **Step 5 - Rename Columns**
  - Renamed columns to lowercase with underscores for consistency.

- **Step 6 - Add Update Timestamp Column**
  - Added `update_timestamp` to track modifications.

### **10/04/2024**

- **Create `requirements.txt` File**
  - Compiled all necessary libraries.
  - **Reference:** [requirements.txt](https://drive.google.com/file/d/1-3kwOmRwreqKCyA_KzZ1hNtlr3pfjY4m/view?usp=sharing)

- **Load Dataset to PostgreSQL Automatically**
  - Implemented automated scripts to load daily job datasets.

- **Append Daily Job Dataset Start Date**
  - Start Date: 10/03/2024

## **Key Considerations**

1. **Secure API Key Storage and Retrieval**
   - **Solution:** Moved API keys to a `.env` file to prevent hardcoding.

2. **PostgreSQL Credentials Storage and Retrieval**
   - **Solution:** Stored PostgreSQL credentials in a `.env` file for security.

3. **Secure S3 Access**
   - **Solution:** Moved S3 access keys to a `.env` file to avoid hardcoding.

4. **Create a `requirements.txt` File**
   - Listed all necessary libraries for the project.
   - **Reference:** [requirements.txt](https://drive.google.com/file/d/1-3kwOmRwreqKCyA_KzZ1hNtlr3pfjY4m/view?usp=sharing)

## **Final Cleaned Job Data File**

- **File:** `cleaned_job_data_9_26.csv`
- **Download Link:** [cleaned_job_data_9_26.csv](https://drive.google.com/file/d/1-6ZFpuKh2YgBn_T73oNHwSReITrnscyf/view?usp=sharing)

## **Questions**

- **Should all columns be retained, or should only the proposed columns be kept while moving data to PostgreSQL?**

  | Column Name         | Sample                                                                                                           |
  |---------------------|-------------------------------------------------------------------------------------------------------------------|
  | Job Key             | `5c42f8e69308ad1a`                                                                                               |
  | Company_search      | `Peraton`                                                                                                         |
  | Title_search        | `Senior Data Analyst and Lead`                                                                                   |
  | Salary Max          | `216000`                                                                                                          |
  | Salary Min          | `135000`                                                                                                          |
  | Salary Type         | `yearly`                                                                                                          |
  | City                | `Fort Lewis`                                                                                                      |
  | State               | `WA`                                                                                                              |
  | Pub Date            | `8/17/24 5:00`                                                                                                     |
  | Currency            | `USD`                                                                                                             |
  | Salary Info         | `$135,000 - $216,000 a year`                                                                                     |
  | Job Type_search     | `Full-time`                                                                                                       |
  | Link                | [Job Listing](https://www.indeed.com/rc/clk?jk=5c42f8e69308ad1a&bb=gncrGbeTB_DudA78atCBYm8BXHx64lJXzTUq9MzWX1NfdUsLDmh5puQPyZ38A9q6B_f0tIezMuNU5jRZnxvf3yYQ1iRxQYfVBhk5CZUQuvY3CWx20eUT_ER4xDfu1hUU&xkcb=SoB967M391ywWOyU650LbzkdCdPP&fccid=7dc8be9efe945d3a&vjs=3) |
  | Job Description     | *(Empty)*                                                                                                         |

## **Creating the `cleaned_job_data_9_26` Table and Importing Data**

1. **Create the Table in PostgreSQL**

   ```sql
   CREATE TABLE job_market_raw.cleaned_job_data_9_26 (
       job_key VARCHAR PRIMARY KEY,
       company_search VARCHAR,
       title_search VARCHAR,
       salary_max DECIMAL,
       salary_min DECIMAL,
       salary_type VARCHAR,
       city VARCHAR,
       state VARCHAR,
       pub_date TIMESTAMP,
       currency VARCHAR,
       salary_info VARCHAR,
       job_type_search VARCHAR,
       link TEXT,
       job_description TEXT,
       update_timestamp TIMESTAMP
   );
