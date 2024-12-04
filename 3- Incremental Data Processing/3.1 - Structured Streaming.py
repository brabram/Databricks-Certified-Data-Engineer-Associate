# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

query = (spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts_tmp_vw

# COMMAND ----------

# Lokalizacja punktu kontrolnego
checkpoint_location = "dbfs:/mnt/demo/author_counts_checkpoint"

# Usunięcie punktu kontrolnego
dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='40 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("Bo", "Introduction to Modeling and Simulation", "ong", "Computer Science", 25),
# MAGIC         ("Ba", "Robot Modeling and Control", "W.", "Computer Science", 30),
# MAGIC         ("Bad", "Turing's Vision: The Birth of Computer Science", "hard", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B106", "Hands-On Deep Learning Algorithms with Python", "xSudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B107", "Neural Network Methods in Natural Language Processing", "xYoav Goldberg", "Computer Science", 30),
# MAGIC         ("B018", "Understanding digital signal processing", "xRichard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts_tmp_vw

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY author_counts
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED author_counts

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/demo/author_counts_checkpoint")

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/author_counts")

display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B17306", "Hands-On Deep Learning Algorithms with Python", "xdS6udharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B41507", "Neural Network Methods in Natural Language Processing", "xdYoa6v Goldberg", "Computer Science", 30),
# MAGIC         ("Bf5018", "Understanding digital signal processing", "xdRichard 6Lyons", "Computer Science", 35)
