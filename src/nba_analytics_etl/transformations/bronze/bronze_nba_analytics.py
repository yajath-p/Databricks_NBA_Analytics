from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
landing_path="/Volumes/workspace/nba_analytics/landing_zone/"
topics = ["games", "games_details", "players", "teams", "ranking"]



def load_bronze(topic):

    @dp.table (
        name="nba_bronze_"+topic
        )
    def load_topic():
        topic_landing_path = landing_path + topic

        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header","true")
            .option("delta.enableChangeDataFeed", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .load(topic_landing_path+"/")
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("time_added", current_timestamp())
        )

for topic in topics:
    load_bronze(topic)

        


    