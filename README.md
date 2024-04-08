# Airline Data Ingestion Pipeline

## Overview
The Airline Data Ingestion project aims to automate the ingestion of daily flights transactional data into a Redshift data warehouse using AWS services. The process begins with data landing in an S3 bucket, which triggers a series of ETL operations orchestrated by Step Functions. Glue is utilized for data extraction, transformation, and loading, ensuring that the data is cleansed, enriched, and ready for analysis.

![Data Architecture](data_architecture.png)

## Steps Involved
1. **S3 Bucket Creation**: A dedicated S3 bucket is created to store incoming daily flights data, organized in a Hive style partitioning format based on date. This ensures efficient data retrieval and management.
2. **Redshift Data Warehouse Setup**: A Redshift data warehouse is provisioned to serve as the central repository for the ingested data. Dimensional tables such as `airports_dim` are established to store airport details, while a fact schema is designed to accommodate flight-related information.
3. **Glue Crawler Configuration**: A Glue crawler is configured to automatically discover and catalog the raw flights data stored in the S3 bucket. Additionally, it extracts the schema of the target database to facilitate data processing.
4. **Visual ETL with Glue Job**: A Visual ETL process is developed using Glue to transform the raw flights data into a structured format. This includes extracting essential fields like origin airport ID, destination airport ID, origin delay, and arrival delay.
5. **Data Filtering**: Filtering conditions are applied to eliminate flights with delays exceeding 60 minutes, ensuring that only relevant data is loaded into the warehouse.
6. **Data Enrichment**: The flights data is enriched by joining it with the airport dimension table to obtain additional details such as departure city, state, and country.
7. **Schema Transformation**: Schema changes are implemented as necessary, dropping any redundant or unnecessary columns from the dataset to optimize storage and query performance.
8. **Additional Enrichment**: Further enrichment of the data is performed by joining it based on destination ID with the airport dimension table to acquire destination details.
9. **Data Loading into Redshift**: The transformed and enriched data is loaded into the Redshift fact table, ensuring proper IAM role permissions are assigned to facilitate data loading.
10. **Pipeline Triggering**: The data pipeline is configured to be triggered automatically upon the arrival of new data in the S3 bucket, ensuring seamless and continuous data processing.
11. **Step Function Orchestration**: Step Functions are leveraged to orchestrate the entire workflow, managing the execution of the Glue crawler, triggering the Glue job, and sending SNS notifications upon job completion.

## Learning and Challenges
The Airline Data Ingestion project provided valuable learning experiences and presented several challenges along the way.

### Learning
- **AWS Service Integration**: I gained hands-on experience in integrating various AWS services such as S3, Glue, Redshift, Step Functions, and SNS to build a robust data pipeline.
- **Data Transformation Techniques**: I enhanced my skills in data transformation techniques using Glue, including filtering, enrichment, and schema transformations to prepare data for analysis.

### Challenges
- **Permissions Management**: Managing IAM roles and permissions to ensure proper access to AWS resources was a significant challenge. Ensuring that the right permissions were granted for data extraction, transformation, and loading processes required careful configuration.
- **Configuration Complexity**: Configuring and fine-tuning the pipeline components, including Glue crawlers, jobs, and Step Functions, presented challenges due to the complexity of orchestrating multiple services to work together seamlessly.
- **Error Handling and Monitoring**: Implementing robust error handling mechanisms and monitoring solutions to detect and address pipeline failures in real-time proved to be challenging, requiring iterative refinement of the pipeline design.

Overall, overcoming these challenges and successfully implementing the Airline Data Ingestion pipeline enhanced my AWS skills and deepened my understanding of building scalable and reliable data pipelines in a cloud environment.
