# nightly_master_ban_list_poc.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# -----------------------------
# 1️⃣ Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("MasterBanListPOC") \
    .getOrCreate()

# -----------------------------
# 2️⃣ Read the POC data from GCS
# -----------------------------
# Your Cloud Function writes to: gs://BUCKET/bank-fingerprints-nightly/year=*/month=*/day=*/
df = spark.read.parquet("gs://BUCKET/bank-fingerprints-nightly/year=*/month=*/day=*/")

# -----------------------------
# 3️⃣ Aggregate metrics per visitor_id
# -----------------------------
banned = (
    df.groupBy("visitor_id")
    .agg(
        countDistinct("user_id").alias("accounts"),
        sum(when(col("chargeback") == True, 1).otherwise(0)).alias("chargebacks"),
        countDistinct("campaign_id").alias("campaigns_abuse"),
        max("ml_fraud_score").alias("worst_ml_score"),
        max(when(col("is_emulator"), 1).otherwise(0)).alias("ever_emulator"),
        max(when(col("is_vpn") | col("is_proxy"), 1).otherwise(0)).alias("ever_proxy"),
        countDistinct("ip").alias("ip_count")
    )
    .filter(
        (col("accounts") >= 12) |
        (col("chargebacks") >= 3) |
        (col("campaigns_abuse") >= 40) |
        (col("worst_ml_score") >= 0.999) |
        (col("ever_emulator") == 1) |
        (col("ever_proxy") == 1) |
        (col("ip_count") >= 200)
    )
    .select("visitor_id")
)

# -----------------------------
# 4️⃣ Write the output
# -----------------------------
# 4a. Write to GCS as text (simulating CDN)
banned.repartition(1).write.mode("overwrite").text("gs://BUCKET/bank-fingerprints-nightly/master_ban_list.txt")

# 4b. Optional: Write to local CSV (for testing)
banned.repartition(1).write.mode("overwrite").csv("./master_ban_list_local.csv", header=True)

# -----------------------------
# 5️⃣ Print summary
# -----------------------------
print(f"NIGHTLY BAN LIST: {banned.count():,} devices banned tonight")
