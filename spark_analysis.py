"""WikiPulse – first Spark DataFrame analysis over collected enwiki data.

Run from the `historical/` dir:
    python3 spark_analysis.py

Reads recentchanges for Sept 16 only (rc_2026-09-16.jsonl.gz) and
prints answers to the core project questions: edit volume, bot vs human,
volatility (edit wars / reverts), hot pages, and size churn.
"""
import os
if "JAVA_HOME" not in os.environ or not os.path.exists(os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java")):
    if os.path.exists("/opt/homebrew/opt/openjdk@17"):
        os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
        os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, functions as F, Window

spark = (SparkSession.builder.appName("WikiPulse")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.caseSensitive", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- Load ----------
rc = (spark.read.json("data/enwiki_recentchanges/rc_2026-09-16.jsonl.gz")
      .withColumn("ts", F.to_timestamp("timestamp"))
      .withColumn("hour", F.date_trunc("hour", "ts"))
      .withColumn("delta", F.col("newlen") - F.col("oldlen"))
      .withColumn("is_revert", F.array_contains("tags", "mw-reverted")
                  | F.array_contains("tags", "mw-manual-revert")
                  | F.array_contains("tags", "mw-rollback")
                  | F.array_contains("tags", "mw-undo"))
      .withColumn("is_anon", (F.coalesce(F.col("temp"), F.lit(False)) == True) 
                             | F.col("user").startswith("~") 
                             | F.col("userid").isNull() 
                             | (F.col("userid") == 0))
      .withColumn("is_anon", F.coalesce(F.col("is_anon"), F.lit(False)))
      .cache())

print("rc rows (2026-09-16):", rc.count())
rc.printSchema()

# ---------- 1. Volume per day / by type ----------
print("== Edits per day by type ==")
(rc.groupBy(F.to_date("ts").alias("day"), "type").count()
   .orderBy("day", "type").show(50, False))

# ---------- 2. Namespace 0 (articles) hourly pulse, bot vs human ----------
print("== Hourly article edits: human vs bot ==")
(rc.filter("ns = 0 AND type IN ('edit','new')")
   .groupBy("hour").agg(
        F.count("*").alias("edits"),
        F.sum(F.col("bot").cast("int")).alias("bot_edits"),
        F.sum(F.col("is_anon").cast("int")).alias("anon_edits"),
        F.sum(F.col("is_revert").cast("int")).alias("reverts"))
   .withColumn("bot_pct", F.round(100 * F.col("bot_edits") / F.col("edits"), 1))
   .orderBy("hour").show(24, False))

# ---------- 3. Hot pages: most edited, distinct editors, reverts ----------
print("== Top 20 hottest article pages ==")
hot = (rc.filter("ns = 0 AND NOT bot")
         .groupBy("pageid", "title").agg(
             F.count("*").alias("edits"),
             F.countDistinct("user").alias("editors"),
             F.sum(F.col("is_revert").cast("int")).alias("reverts"),
             F.sum(F.abs("delta")).alias("bytes_churn"))
         .withColumn("revert_ratio", F.round(F.col("reverts") / F.col("edits"), 3)))
hot.orderBy(F.desc("edits")).show(20, False)

# ---------- 4. Controversy score (edit-war proxy) ----------
# many edits + many editors + high revert ratio, with a minimum volume floor
print("== Top 20 controversy candidates ==")
(hot.filter("edits >= 20")
    .withColumn("controversy", F.round(
        F.log1p("edits") * F.log1p("editors") * (F.col("revert_ratio") + 0.05), 3))
    .orderBy(F.desc("controversy")).show(20, False))

# ---------- 5. Burst detection: hourly z-score per page ----------
print("== Pages with sudden edit bursts (z-score on hourly counts) ==")
hourly = (rc.filter("ns = 0").groupBy("pageid", "title", "hour").count())
w = Window.partitionBy("pageid")
(hourly.withColumn("mu", F.avg("count").over(w))
       .withColumn("sd", F.stddev("count").over(w))
       .withColumn("z", (F.col("count") - F.col("mu")) / F.col("sd"))
       .filter("count >= 10 AND z > 3")
       .orderBy(F.desc("z")).show(20, False))

# ---------- 6. Top editors & edit-size distribution ----------
print("== Top 15 human editors ==")
(rc.filter("NOT bot AND NOT is_anon").groupBy("user").count()
   .orderBy(F.desc("count")).show(15, False))

print("== Edit-size percentiles (article namespace) ==")
rc.filter("ns = 0").selectExpr(
    "percentile_approx(delta, array(0.01,0.25,0.5,0.75,0.99)) AS pct").show(truncate=False)

# ---------- 7. Tags (editing channels) ----------
print("== Top edit tags ==")
(rc.select(F.explode("tags").alias("tag")).groupBy("tag").count()
   .orderBy(F.desc("count")).show(20, False))

# ---------- Save curated output as Parquet for later notebooks ----------
hot.write.mode("overwrite").parquet("data/curated/hot_pages")
spark.stop()
