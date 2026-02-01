# Databricks notebook source
# MAGIC %md
# MAGIC ### Project Set Up Notebook
# MAGIC
# MAGIC This splits up the data and creates schema/volume.
# MAGIC
# MAGIC **Note**: This notebook is not executed as part of the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC FIRST CREATE VOLUMES AND SCHEMA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.nba_analytics;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.nba_analytics.landing_zone;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.nba_analytics.data_splitter;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC THEN TO SIMULATE AUTOLOADER FUNCTIONALITY SPLIT UP DATA OF EACH TABLE

# COMMAND ----------

volume_path = "/Volumes/workspace/nba_analytics/data_splitter"
chunks = df.randomSplit([0.2, 0.2, 0.2, 0.2, 0.2])
email = ""

# COMMAND ----------


df = spark.read.csv(f"/Workspace/Users/{email}/nba_analytics/src/nba_analytics_etl/explorations_and_setup/raw_data/players.csv", header=True)

for i, chunk in enumerate(chunks):
    
    chunk.write.format("csv").option("header", "true").mode("overwrite") \
         .save(f"{volume_path}/players/batch_{i}")
    
    print(f"Batch {i} is now live in the Volume!")


# COMMAND ----------


df = spark.read.csv(f"/Workspace/Users/{email}/nba_analytics/src/nba_analytics_etl/explorations_and_setup/raw_data/games.csv", header=True)

for i, chunk in enumerate(chunks):
    
    chunk.write.format("csv").option("header", "true").mode("overwrite") \
         .save(f"{volume_path}/games/batch_{i}")
    
    print(f"Batch {i} is now live in the Volume!")


# COMMAND ----------


df = spark.read.csv(f"/Workspace/Users/{email}/nba_analytics/src/nba_analytics_etl/explorations_and_setup/raw_data/ranking.csv", header=True)

for i, chunk in enumerate(chunks):
    
    chunk.write.format("csv").option("header", "true").mode("overwrite") \
         .save(f"{volume_path}/ranking/batch_{i}")
    
    print(f"Batch {i} is now live in the Volume!")

# COMMAND ----------


df = spark.read.csv(f"/Workspace/Users/{email}/nba_analytics/src/nba_analytics_etl/explorations_and_setup/raw_data/teams.csv", header=True)

for i, chunk in enumerate(chunks):
    
    chunk.write.format("csv").option("header", "true").mode("overwrite") \
         .save(f"{volume_path}/teams/batch_{i}")
    
    print(f"Batch {i} is now live in the Volume!")

# COMMAND ----------


df = spark.read.csv(f"/Workspace/Users/{email}/nba_analytics/src/nba_analytics_etl/explorations_and_setup/raw_data/games_details.csv", header=True)

for i, chunk in enumerate(chunks):
    
    chunk.write.format("csv").option("header", "true").mode("overwrite") \
         .save(f"{volume_path}/games_details/batch_{i}")
    
    print(f"Batch {i} is now live in the Volume!")

# COMMAND ----------

# MAGIC %md
# MAGIC THEN LOAD DATA TO TRIGGER AUTOLOADER BATCH NUMBER HAS TO BE MANUALLY SET

# COMMAND ----------

batch_number = 0
topics_list = ["games","games_details","players","ranking","teams"]

# COMMAND ----------

for topic in topics_list:
    curr_path="/Volumes/workspace/nba_analytics/data_splitter/"+topic+"/batch_"+str(batch_number)
    end_path="/Volumes/workspace/nba_analytics/landing_zone/"+topic
    dbutils.fs.cp(curr_path, end_path, recurse=True)
    
