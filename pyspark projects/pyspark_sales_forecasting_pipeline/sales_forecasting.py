#Imports and Initial Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, dayofweek, avg, lag, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import to_date
from pyspark.ml.evaluation import RegressionEvaluator

# Start Spark session
spark = SparkSession.builder \
    .appName("Sales Forecasting Pipeline") \
    .getOrCreate()

#Extract Historical Data
# Load historical sales data
data = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

#Create Temporal Features
# Convert date column to date type
data = data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Extract month and day of week
data = data.withColumn("month", month(col("date")))
data = data.withColumn("day_of_week", dayofweek(col("date")))

# Create holiday feature (example: 1 if Sunday)
data = data.withColumn("holiday", when(col("day_of_week") == 1, 1).otherwise(0))

# 7-day moving average by store and product
window_spec = Window.partitionBy("store", "product").orderBy("date").rowsBetween(-6, 0)
data = data.withColumn("7d_moving_avg", avg(col("sales")).over(window_spec))

# Drop initial nulls from moving average
data = data.na.drop()

#Assemble Feature Vector
feature_cols = ["month", "day_of_week", "holiday", "7d_moving_avg"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

#Train Linear Regression Model
# Define linear regression model
lr = LinearRegression(featuresCol="features", labelCol="sales")

# Create full pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Split data into training and test sets
train, test = data.randomSplit([0.8, 0.2], seed=42)

# Train the model
model = pipeline.fit(train)

# Make predictions
predictions = model.transform(test)

# Show results
predictions.select("date", "sales", "prediction").show(10)

#Evaluate the Model
evaluator = RegressionEvaluator(
    labelCol="sales", predictionCol="prediction", metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
print(f"Model RMSE: {rmse:.2f}")