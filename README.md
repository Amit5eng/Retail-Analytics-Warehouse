&#x20;\*\*Retail Analytics Warehouse\*\*

\--------------------------------

An end-to-end data engineering project that transforms retail sales data into analytics-ready datasets using PySpark, Apache Airflow, AWS S3, Snowflake, and dbt.



&#x20;\*\*Project Overview\*\*

\----------------------



The Retail Analytics Warehouse automates the processing of monthly retail sales data, from raw CSV ingestion to business-ready analytical tables. The pipeline performs data cleaning, validation, deduplication, cloud storage, warehouse loading, dimensional modeling, and automated data quality checks.



\*\*Key results\*\*

\- Processed 10,000 retail sales transactions covering Januaryâ€“December 2025.

\- Loaded sales and master data into Snowflake.

\- Built dimensional models and analytical tables using dbt.

\- Passed all 33 dbt data tests in the successful Airflow run.

\- Orchestrated the pipeline using a six-task Airflow DAG.



&#x20;\*\*Architecture\*\*

\------------------



Monthly Retail CSV Files

&#x20;         |

&#x20;         v

&#x20;  Python + PySpark

&#x20;Ingestion, Cleaning,

&#x20;Validation, Deduplication

&#x20;         |

&#x20;         v

&#x20;   Processed Parquet

&#x20;         |

&#x20;         +--------------------+

&#x20;         |                    |

&#x20;         v                    v

&#x20;      AWS S3              Snowflake

&#x20;  Raw + Processed       Staging Tables

&#x20;                              |

&#x20;                              v

&#x20;                        dbt Transformations

&#x20;                              |

&#x20;                              v

&#x20;                    Analytics Data Warehouse

&#x20;                              |

&#x20;             +----------------+----------------+

&#x20;             |                |                |

&#x20;             v                v                v

&#x20;         Fact Sales      Dimensions       Analytics Marts



Airflow orchestrates the pipeline, dependencies, retries,

and execution monitoring.





&#x20;\*\*Technology Stack\*\*

\----------------------

Technology and  Purpose

\--------------------------

&#x20;Python - Data processing and pipeline utilities

&#x20;PySpark - Distributed data transformation, validation, and deduplication

&#x20;Apache Airflow - Workflow orchestration, dependencies, retries, and monitoring

&#x20;AWS S3 - Cloud storage for raw CSV and processed Parquet data

&#x20;Snowflake - Cloud data warehouse and analytical storage

&#x20;dbt Core - SQL transformations, dimensional modeling, and data testing

&#x20;SQL - Data validation, transformations, and analytical queries

&#x20;Docker - Containerized Airflow environment

&#x20;Git \& GitHub - Version control and project documentation



&#x20;\*\*Pipeline Workflow\*\*

\-----------------------



1\. Batch ETL: Reads monthly sales CSV files, applies data cleaning and validation rules, handles duplicate transaction IDs, and writes processed Parquet data.

2\. AWS S3 Upload: Uploads raw sales files and processed data to the configured S3 bucket.

3\. Master Data Loading: Loads product, customer, and store reference data into Snowflake staging tables.

4\. Sales Data Loading: Loads processed sales transactions into Snowflake with transaction-level validation and merge logic.

5\. dbt Run: Builds staging and intermediate models, dimensions, the sales fact table, and analytical marts.

6\. dbt Test: Runs automated data quality checks, including uniqueness, not-null, accepted-value, and relationship tests.



&#x20;\*\*Data Modeling\*\*

\-------------------

The Snowflake ANALYTICS schema contains eight verified analytical tables:



\- `FACT\_SALES` â€” retail transaction fact table.

\- `DIM\_DATE` â€” date dimension.

\- `DIM\_PRODUCTS` â€” product dimension.

\- `DIM\_CUSTOMERS` â€” customer dimension.

\- `DIM\_STORES` â€” store dimension.

\- `MONTHLY\_SALES\_SUMMARY` â€” monthly sales analysis.

\- `PRODUCT\_PERFORMANCE` â€” product-level performance metrics.

\- `STORE\_PERFORMANCE` â€” store-level performance metrics.



The dimensional model supports reporting and analysis across time, products, customers, and stores.



&#x20;\*\*Validation and Results Metric and Verified result\*\*

\-------------------------------------------------------

&#x20;Source sales period - Januaryâ€“December 2025

&#x20;Processed sales transactions - 10,000

&#x20;Unique transaction IDs in `FACT\_SALES` - 10,000

&#x20;Analytics tables - 8

&#x20;dbt data tests passed - 33

&#x20;dbt warnings - 0

&#x20;dbt errors - 0

&#x20;Final Airflow DAG run - Successful



The fact-table row count and unique transaction count were verified in Snowflake. The dbt test results were verified from the successful Airflow task logs.



&#x20;\*\*Project Structure\*\*

\-----------------------



Retail\_Analytics\_Warehouse/

â”œâ”€â”€ config/

â”‚   â””â”€â”€ profiles.yml

â”œâ”€â”€ dags/

â”‚   â””â”€â”€ retail\_warehouse.py

â”œâ”€â”€ data/

â”‚   â”œâ”€â”€ raw/

â”‚   â”œâ”€â”€ processed/

â”‚   â””â”€â”€ rejected/

â”œâ”€â”€ dbt/

â”‚   â”œâ”€â”€ models/

â”‚   â”‚   â”œâ”€â”€ staging/

â”‚   â”‚   â”œâ”€â”€ intermediate/

â”‚   â”‚   â””â”€â”€ marts/

â”‚   â”œâ”€â”€ macros/

â”‚   â””â”€â”€ dbt\_project.yml

â”œâ”€â”€ sql/

â”œâ”€â”€ src/

â”‚   â”œâ”€â”€ batch\_etl.py

â”‚   â”œâ”€â”€ generate\_retail\_data.py

â”‚   â”œâ”€â”€ load\_master\_data.py

â”‚   â”œâ”€â”€ s3\_uploader.py

â”‚   â””â”€â”€ snowflake\_loader.py

â”œâ”€â”€ tests/

â”œâ”€â”€ .gitignore

â””â”€â”€ README.md





&#x20;\*\*Skills Demonstrated\*\*

\-------------------------

\- Batch data engineering and ETL development

\- PySpark transformations and data quality validation

\- Apache Airflow DAG development and orchestration

\- AWS S3 data lake integration

\- Snowflake data loading and dimensional modeling

\- dbt model development and automated testing

\- SQL-based validation and analytical data preparation

\- Docker-based development environments



&#x20;\*\*Future Enhancements\*\*

\-------------------------

\- Add incremental processing and automated recovery for failed batches.

\- Integrate CI/CD checks for Python, SQL, and dbt changes.

\- Add monitoring dashboards and data freshness checks.



&#x20;Author

\--------

\*\*Amit Kumar Chaudhary\*\*



Data Engineering | Python | SQL | PySpark | Apache Airflow | AWS | Snowflake | dbt



GitHub: \[Amit5eng](https://github.com/Amit5eng)



