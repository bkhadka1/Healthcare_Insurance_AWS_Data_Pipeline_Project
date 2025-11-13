import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# S3 bucket paths
RAW_PATH = "s3://healthcare-insurance-data/raw/"
CLEANED_PATH = "s3://healthcare-insurance-data/cleaned/"

# ===== 1. CLEAN CLAIMS DATA =====
print("Cleaning claims data...")

# Read claims JSON
claims_df = spark.read.json(f"{RAW_PATH}claims.json")

# Clean claims
claims_cleaned = claims_df \
    .withColumn("claim_id", col("claim_id").cast(IntegerType())) \
    .withColumn("patient_id", col("patient_id").cast(IntegerType())) \
    .withColumn("claim_amount", 
                when(col("claim_amount").isNotNull(), 
                     col("claim_amount").cast(IntegerType()))
                .otherwise(0)) \
    .withColumn("claim_date", to_date(col("claim_date"), "yyyy-MM-dd")) \
    .withColumn("Claim_Or_Rejected", 
                when(col("Claim_Or_Rejected") == "NaN", None)
                .otherwise(col("Claim_Or_Rejected"))) \
    .withColumn("claim_status",
                when(col("Claim_Or_Rejected") == "Y", "Rejected")
                .when(col("Claim_Or_Rejected") == "N", "Approved")
                .otherwise("Pending")) \
    .dropDuplicates(["claim_id"])

# Write to S3 as Parquet (partitioned by year for better performance)
claims_cleaned.write \
    .mode("overwrite") \
    .partitionBy("claim_status") \
    .parquet(f"{CLEANED_PATH}claims/")

print(f"Claims cleaned: {claims_cleaned.count()} records")


# ===== 2. CLEAN DISEASE DATA =====
print("Cleaning disease data...")

disease_df = spark.read.option("header", "true").csv(f"{RAW_PATH}disease.csv")

disease_cleaned = disease_df \
    .dropDuplicates(["Disease_ID"])

disease_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}disease/")
print(f"Disease data cleaned: {disease_cleaned.count()} records")


# ===== 3. CLEAN GROUP DATA =====
print("Cleaning group data...")

group_df = spark.read.option("header", "true").csv(f"{RAW_PATH}group.csv")

group_cleaned = group_df \
    .withColumn("premium_written", col("premium_written").cast(IntegerType())) \
    .withColumn("zipcode", col("zipcode").cast(IntegerType())) \
    .withColumn("year", col("year").cast(IntegerType())) \
    .withColumn("Grp_Type", 
                when(col("Grp_Type") == "Govt.", "Government")
                .otherwise("Private")) \
    .dropDuplicates(["Grp_Id"])

group_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}group/")
print(f"Group data cleaned: {group_cleaned.count()} records")


# ===== 4. CLEAN HOSPITAL DATA =====
print("Cleaning hospital data...")

hospital_df = spark.read.option("header", "true").csv(f"{RAW_PATH}hospital.csv")

hospital_cleaned = hospital_df \
    .withColumn("state", 
                when(col("state") == "NaN", None)
                .otherwise(col("state"))) \
    .dropDuplicates(["Hospital_id"])

hospital_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}hospital/")
print(f"Hospital data cleaned: {hospital_cleaned.count()} records")


# ===== 5. CLEAN PATIENT RECORDS =====
print("Cleaning patient records...")

patient_df = spark.read.option("header", "true").csv(f"{RAW_PATH}Patient_records.csv")

patient_cleaned = patient_df \
    .withColumn("Patient_id", col("Patient_id").cast(IntegerType())) \
    .withColumn("patient_birth_date", to_date(col("patient_birth_date"), "yyyy-MM-dd")) \
    .withColumn("age", 
                floor(datediff(current_date(), col("patient_birth_date")) / 365.25)) \
    .withColumn("Patient_name",
                when(col("Patient_name").isNull(), "Unknown")
                .otherwise(col("Patient_name"))) \
    .dropDuplicates(["Patient_id"])

patient_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}patient_records/")
print(f"Patient records cleaned: {patient_cleaned.count()} records")


# ===== 6. CLEAN SUBSCRIBER DATA =====
print("Cleaning subscriber data...")

subscriber_df = spark.read.option("header", "true").csv(f"{RAW_PATH}subscriber.csv")

subscriber_cleaned = subscriber_df \
    .withColumn("Birth_date", to_date(col("Birth_date"), "yyyy-MM-dd")) \
    .withColumn("eff_date", to_date(col("eff_date"), "yyyy-MM-dd")) \
    .withColumn("term_date", to_date(col("term_date"), "yyyy-MM-dd")) \
    .withColumn("age", 
                floor(datediff(current_date(), col("Birth_date")) / 365.25)) \
    .withColumn("Elig_ind",
                when(col("Elig_ind").isNull(), "N")
                .otherwise(col("Elig_ind"))) \
    .withColumn("Subgrp_id",
                when(col("Subgrp_id").isNull(), "Unknown")
                .otherwise(col("Subgrp_id"))) \
    .dropDuplicates(["sub_id"])

subscriber_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}subscriber/")
print(f"Subscriber data cleaned: {subscriber_cleaned.count()} records")


# ===== 7. CLEAN SUBGROUP DATA =====
print("Cleaning subgroup data...")

subgroup_df = spark.read.option("header", "true").csv(f"{RAW_PATH}subgroup.csv")

subgroup_cleaned = subgroup_df \
    .withColumn("Monthly_Premium", col("Monthly_Premium").cast(IntegerType())) \
    .dropDuplicates(["SubGrp_id"])

subgroup_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}subgroup/")
print(f"Subgroup data cleaned: {subgroup_cleaned.count()} records")


# ===== 8. CLEAN GROUP-SUBGROUP MAPPING =====
print("Cleaning group-subgroup mapping...")

grpsubgrp_df = spark.read.option("header", "true").csv(f"{RAW_PATH}grpsubgrp.csv")

grpsubgrp_cleaned = grpsubgrp_df.dropDuplicates()

grpsubgrp_cleaned.write.mode("overwrite").parquet(f"{CLEANED_PATH}grpsubgrp/")
print(f"Group-Subgroup mapping cleaned: {grpsubgrp_cleaned.count()} records")


# ===== 9. CREATE ANALYTICS TABLES =====
print("Creating analytics tables...")

# Create comprehensive view joining all tables
analytics_df = claims_cleaned \
    .join(patient_cleaned, "patient_id", "left") \
    .join(hospital_cleaned, "hospital_id", "left") \
    .join(disease_cleaned, 
          claims_cleaned.disease_name == disease_cleaned.Disease_name, 
          "left") \
    .join(subscriber_cleaned,
          claims_cleaned.SUB_ID == subscriber_cleaned.sub_id,
          "left") \
    .join(subgroup_cleaned,
          subscriber_cleaned.Subgrp_id == subgroup_cleaned.SubGrp_id,
          "left") \
    .select(
        claims_cleaned["*"],
        patient_cleaned["Patient_name"],
        patient_cleaned["patient_gender"],
        patient_cleaned["age"].alias("patient_age"),
        patient_cleaned["city"].alias("patient_city"),
        hospital_cleaned["Hospital_name"],
        hospital_cleaned["state"].alias("hospital_state"),
        disease_cleaned["SubGrpID"],
        subgroup_cleaned["SubGrp_Name"],
        subgroup_cleaned["Monthly_Premium"],
        subscriber_cleaned["Elig_ind"]
    )

analytics_df.write \
    .mode("overwrite") \
    .partitionBy("claim_status") \
    .parquet(f"{CLEANED_PATH}analytics_consolidated/")

print(f"Analytics table created: {analytics_df.count()} records")

# Data quality checks
print("\n=== DATA QUALITY SUMMARY ===")
print(f"Total Claims: {claims_cleaned.count()}")
print(f"Rejected Claims: {claims_cleaned.filter(col('claim_status') == 'Rejected').count()}")
print(f"Approved Claims: {claims_cleaned.filter(col('claim_status') == 'Approved').count()}")
print(f"Pending Claims: {claims_cleaned.filter(col('claim_status') == 'Pending').count()}")
print(f"Total Claim Amount: {claims_cleaned.agg(sum('claim_amount')).collect()[0][0]}")

job.commit()
print("\nETL Job completed successfully!")