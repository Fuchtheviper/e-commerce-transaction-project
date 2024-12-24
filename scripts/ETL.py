from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofweek, when, round, explode
from awsglue.dynamicframe import DynamicFrame

def extract_data(glueContext, database_name, table_name):
    """
    Extract data from Glue table.
    """
    print(f"Extracting data from Glue table: {database_name}.{table_name}")
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name
    )
    # Convert DynamicFrame to DataFrame for transformations
    df = dynamic_frame.toDF()
    return df

def transform_data(df):
    """
    Clean, deduplicate, and transform the raw data.
    """
    print("Cleaning and ensuring date column...")
    df_cleaned = (
        df.dropDuplicates()                               # Remove duplicates
          .dropna()                                       # Remove Nulls
          .filter(df["Transaction_Date"].isNotNull())     # Filter null dates
          .withColumn("Transaction_Date", to_date(col("Transaction_Date")))  # Parse date
    )

    # Feature engineering
    df_featured = (
        df_cleaned
        .withColumn("Discount_Amount", round((col("Revenue") / (1 - col("Discount_Applied"))) * col("Discount_Applied"), 2))
        .withColumn("Year", year(col("Transaction_Date")))
        .withColumn("Month", month(col("Transaction_Date")))
        .withColumn("Day_of_Week", dayofweek(col("Transaction_Date")))
        .withColumn("Is_Weekend", when(col("Day_of_Week").isin(6, 7), 1).otherwise(0))
    )
    return df_featured

def load_data_to_s3(df, output_path):
    """
    Load transformed data into S3 in Parquet format with partitioning.
    """
    print(f"Loading data to S3 at: {output_path}")
    df.write.mode("overwrite").partitionBy("Year", "Month").parquet(output_path)
    print(f"Data successfully written to S3: {output_path}")

def main():
    # Define Glue table and output path
    database_name = "raw-e-commerce-stream-transaction"
    table_name = "batched_data"
    output_path = "s3://e-commerce-datalake/processed/featured_data/"

    # GlueContext is pre-configured in Glue jobs
    glueContext = GlueContext(SparkSession.builder.getOrCreate())
    spark = glueContext.spark_session

    # ETL Workflow
    raw_data = extract_data(glueContext, database_name, table_name)
    transformed_data = transform_data(raw_data)
    load_data_to_s3(transformed_data, output_path)

    print("ETL job completed successfully!")

if __name__ == "__main__":
    main()