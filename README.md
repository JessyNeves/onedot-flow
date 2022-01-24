# OneDot Challenge
Resolution proposal for OneDot challenge

This spark pipeline was built in a simple way given the simplicity of the transformations. 

Production wise it would be recommendable to split the steps into actual different jobs, or modules. 
This could be easily achievable by, for example, using tools like Airflow/Dagster, Glue Workflows or Databricks to orchestrate these steps.
By doing so we would be able to implement monitoring and even data quality over the different phases of the pipeline, development would be easier and debugging faster.


# 1. Installation

  ### 1.1 Dependencies
   - DOCKER (run & build)
   https://www.docker.com/
  
  ### 1.2 Build
  Once the repository is downloaded browse to the directory and build the docker image (this can take up to 3 minutes, depending on internet connection and machine):

  - ```sudo docker build -t onedot-challenge .```

 # 2. Run
 
  After the build, run the docker image:
 
  - ```docker run -v $PWD/csv:/csv onedot-challenge```
  
# 3. Ouput
  The answers will be written to the project folder CSV.
  There you will find 4 CSV files, one for each step: Pre-Processing, Normalization, Extraction and Integration
  
# 4. STEPS
  ### 4.1 Pre-Processing
  - Simple data read with escape characters and encoding in mind.
  - Pivot the raw data in order to achieve sink/destination granularity

  ### 4.2 Normalization
  - Built a small configurable normalization engine (common_lib).
  - You can add more columns to normalize and its corresponding mapping/normalization rules in .configuration/normalization.json

  ### 4.3 Extraction
  - Simple extraction/split of values to obtain the desired result.

  ### 4.4 Integration
  - Dropped unnecessary columns and Renamed the present ones. 
  - Enriched the dataframe with sink columns that were not present in source data.
  - This step is configurable in .configuration/integration.json.
  - You can specify which columns to drop, which to rename (and to what) and with to add (and with what).

  ### 4.5 Enrich/Product Matching
  - The first challenge that comes to mind after inspecting the SINK/Destination data is the missing ID. 
    There is no UUID for the vehicles and therefore product matching will have to be done by vehicle parameters (make, type, drive, fuelType, door, etc.)

  - One could use different approaches to deal with this Product Matching. For example, assuming we are building/maintaining a Data Warehouse that does not rely on a relational database,
    we could use Delta Lake (which is built over parquet files). With Delta Lake we would be able to get ACID transactions over parquet files, meaning we could then apply UPSERTS to our 
    sink/destination parquet tables. 
    By identifying a set of keys to define a composite primary key, we could then match the products from the source to the sink, and apply an UPSERT. This would insert products 
    missing in the sink and update the already existing ones. 

    In the case of a relational database the process would be similar, with the difference that we would in fact know the keys to indentify a product. Making use of a jdbc connection to the database,
    we would be able to UPSERT our ready-to-be-integrated source data into our sink table (living somewhere in a relational database).  