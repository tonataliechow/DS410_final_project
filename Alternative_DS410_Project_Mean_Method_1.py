#!/usr/bin/env python
# coding: utf-8

# # Data Preprocessing

# In[1]:


import pyspark
import pandas as pd
import numpy as np
import math


# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, column, when, avg, mean
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql.functions import array_contains, array_position
from pyspark.sql.functions import collect_list
from pyspark.sql import Row
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import DecisionTreeRegressor


# In[3]:


ss=SparkSession.builder.appName("DS410 Project").getOrCreate()


# In[4]:


ss.sparkContext.setCheckpointDir("~/scratch")


# In[5]:


df = ss.read.csv("/storage/home/tah5808/DS410 Project/water_quality_small.csv", header = True, inferSchema = True) # Make sure to use the small dataset


# In[6]:


#df.printSchema()


# In[7]:


df2 = df.select("ActivityStartDate", "MonitoringLocationIdentifier", "CharacteristicName", "ResultMeasureValue", "HydrologicEvent", "ResultMeasureUnits")


# In[8]:


#df2.printSchema()


# In[9]:


#df2.show(5)


# In[10]:


df3 = df2.filter(col("ActivityStartDate").isNotNull())
df4 = df3.filter(col("MonitoringLocationIdentifier").isNotNull())


# In[11]:


#df4.filter(col("CharacteristicName") == "Temperature, water").select("ResultMeasureUnits").summary().show()


# In[12]:


#df4.filter(col("CharacteristicName") == "Temperature, water").groupBy("ResultMeasureUnits").count().show()


# In[13]:


#df4.filter(col("CharacteristicName") == "Oxygen").groupBy("ResultMeasureUnits").count().show()
#df4.filter(col("CharacteristicName") == "Oxygen").filter(col("ResultMeasureUnits") == "mg/l").select("ResultMeasureValue").summary().show()
#df4.filter(col("CharacteristicName") == "Oxygen").filter(col("ResultMeasureUnits") == "% saturatn").select("ResultMeasureValue").summary().show()


# In[14]:


#df4.filter(col("CharacteristicName") == "Specific conductance").groupBy("ResultMeasureUnits").count().show()


# In[15]:


#df4.filter(col("CharacteristicName") == "pH").groupBy("ResultMeasureUnits").count().show()
#df4.filter(col("CharacteristicName") == "pH").filter(col("ResultMeasureUnits") == "None").select("ResultMeasureValue").summary().show()
#df4.filter(col("CharacteristicName") == "pH").filter(col("ResultMeasureUnits") == "std units").select("ResultMeasureValue").summary().show()


# In[16]:


#temp = df4.filter(col("CharacteristicName") == "pH")
#temp = df4.filter(col("ResultMeasureUnits") == "std units")


# In[17]:


#temp = temp.withColumn("pH", temp["ResultMeasureValue"].cast(FloatType())).select("pH")


# In[18]:


#temp.summary().show()


# In[19]:


df4 = df4.filter((col("HydrologicEvent") == "Storm") | (col("HydrologicEvent") == "Routine sample"))


# In[20]:


df4 = df4.filter((col("ResultMeasureUnits") == "std units") |                (col("ResultMeasureUnits") == "deg C") |                (col("ResultMeasureUnits") == "mg/l") |                (col("ResultMeasureUnits") == "uS/cm @25C"))


# In[21]:


df_characteristic = df4.groupBy("ActivityStartDate", "MonitoringLocationIdentifier").agg(collect_list("CharacteristicName"))


# In[22]:


df_value = df4.groupBy("ActivityStartDate", "MonitoringLocationIdentifier").agg(collect_list("ResultMeasureValue"))


# In[23]:


df_hydro_event = df4.groupBy("ActivityStartDate", "MonitoringLocationIdentifier").agg(collect_list("HydrologicEvent"))


# In[24]:


df5 = df_characteristic.join(df_value, ["ActivityStartDate", "MonitoringLocationIdentifier"]).join(df_hydro_event, ["ActivityStartDate", "MonitoringLocationIdentifier"])


# In[25]:


#df5.show()


# In[26]:


features = ["Temperature, water", "Specific conductance", "pH", "Oxygen", "HydrologicEvent", "ResultMeasure/MeasureUnitCode"]


# In[27]:


df6 = df5.withColumn(features[0],                     when(array_position("collect_list(CharacteristicName)", features[0]) != 0,                         col("collect_list(ResultMeasureValue)")[array_position("collect_list(CharacteristicName)", features[0]) - 1]
                         ))


# In[28]:


df7 = df6.withColumn(features[1],                     when(array_position("collect_list(CharacteristicName)", features[1]) != 0,                         col("collect_list(ResultMeasureValue)")[array_position("collect_list(CharacteristicName)", features[1]) - 1]
                         ))


# In[29]:


df8 = df7.withColumn(features[2],                     when(array_position("collect_list(CharacteristicName)", features[2]) != 0,                         col("collect_list(ResultMeasureValue)")[array_position("collect_list(CharacteristicName)", features[2]) - 1]
                         ))


# In[30]:


df9 = df8.withColumn(features[3],                     when(array_position("collect_list(CharacteristicName)", features[3]) != 0,                         col("collect_list(ResultMeasureValue)")[array_position("collect_list(CharacteristicName)", features[3]) - 1]
                         ))


# In[31]:


df10 = df9.withColumn(features[4],                     when(array_position("collect_list(HydrologicEvent)", "Storm") != 0, 1).otherwise(0))


# In[32]:


#df10.show(5)


# In[33]:


#df10.columns


# In[34]:


#df10.filter(col("pH") > 14).select("pH").show()


# In[35]:


# # Convert from string to float
df11 = df10.withColumn("Temperature, water", df10["Temperature, water"].cast(FloatType()))
df12 = df11.withColumn("Specific conductance", df11["Specific conductance"].cast(FloatType()))
df13 = df12.withColumn("pH", df12["pH"].cast(FloatType()))
df14 = df13.withColumn("Oxygen", df13["Oxygen"].cast(FloatType()))
df15 = df14.withColumn("HydrologicEvent", df14["HydrologicEvent"].cast(FloatType()))


# In[36]:


#df15.show()


# In[37]:


#df15.filter(col("pH").isNull()).count()


# In[38]:


df16 = df15.filter(col("pH").isNotNull())


# In[39]:


df17 = df16.select("Temperature, water", "Specific conductance", "Oxygen", "HydrologicEvent", "pH")


# In[40]:


#df17.printSchema()


# In[41]:


#df17.show(5)


# In[42]:


#df17.count()


# In[43]:


df17.count()


# # Modeling

# ## Method: Replace all NULL values with mean in corresponding col

# In[44]:


ml_df = pd.DataFrame(columns = ["Model", "RMSE", "R2"])


# In[45]:


# Split the data into training and testing sets
# https://towardsdatascience.com/building-a-linear-regression-with-pyspark-and-mllib-d065c3ba246a
(training_data, validation_data, test_data) = df17.randomSplit([0.6, 0.2, 0.2], seed = 123)


# In[46]:


mean_temperature = training_data.agg(mean(col("Temperature, water"))).collect()[0][0]
mean_conductance = training_data.agg(mean(col("Specific conductance"))).collect()[0][0]
mean_oxygen = training_data.agg(mean(col("Oxygen"))).collect()[0][0]


# In[47]:


#print(mean_temperature)
#print(mean_conductance)
#print(mean_oxygen)


# In[48]:


training_data = training_data.na.fill({"Temperature, water": mean_temperature, "Specific conductance": mean_conductance, "Oxygen": mean_oxygen})


# In[49]:


validation_data = validation_data.na.fill({"Temperature, water": mean_temperature, "Specific conductance": mean_conductance, "Oxygen": mean_oxygen})


# In[50]:


test_data = test_data.na.fill({"Temperature, water": mean_temperature, "Specific conductance": mean_conductance, "Oxygen": mean_oxygen})


# ### Standardization

# In[51]:


input_columns = df17.columns[:-1]
output_columns = df17.columns[-1]


# In[52]:


# Create a feature vector by combining all feature columns into a single 'features' column
assembler = VectorAssembler(inputCols = input_columns, outputCol = 'features')
training_data = assembler.transform(training_data)
validation_data = assembler.transform(validation_data)
test_data = assembler.transform(test_data)


# In[53]:


# Scale the feature vector using StandardScaler
scaler = StandardScaler(inputCol = "features", outputCol = "scaled_features", withStd = True, withMean = True)
scaler_model = scaler.fit(training_data)
training_data = scaler_model.transform(training_data)
validation_data = scaler_model.transform(validation_data)
test_data = scaler_model.transform(test_data)


# ### Ridge Linear Regression

# In[54]:


# Training
model = LinearRegression(featuresCol = "scaled_features", labelCol = "pH", predictionCol = "predicted_pH", elasticNetParam = 0)
lr_model = model.fit(training_data)


# In[55]:


lr_predictions = lr_model.transform(validation_data)

lr_evaluator_rmse = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "rmse")
lr_evaluator = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "r2")

ml_df.loc[len(ml_df)] = {"Model": "Ridge Regression", "RMSE": lr_evaluator_rmse.evaluate(lr_predictions), "R2": lr_evaluator.evaluate(lr_predictions)}

print("RMSE on validation data : %g" % lr_evaluator_rmse.evaluate(lr_predictions))
print("r2 on validation data : %g" % lr_evaluator.evaluate(lr_predictions))


# In[56]:


coefficients = lr_model.coefficients.toArray().tolist()
intercept = [lr_model.intercept]
data = {"Temperature, water": coefficients[0], "Specific conductance": coefficients[1], "Oxygen": coefficients[2], "HydrologicEvent": coefficients[3], "Intercept": intercept}

coefficients_df = pd.DataFrame(data)

#print(coefficients_df)


# In[57]:


output_path = "/storage/home/tah5808/DS410 Project/Linear_Coefficients.csv"
coefficients_df.to_csv(output_path)


# ### Decision Tree

# In[58]:


# Training
dt = DecisionTreeRegressor(featuresCol = "scaled_features", labelCol = "pH", predictionCol = "predicted_pH")
dt_model = dt.fit(training_data)


# In[59]:


# Testing
dt_predictions = dt_model.transform(validation_data)

dt_evaluator_rmse = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "rmse")
dt_evaluator = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "r2")

ml_df.loc[len(ml_df)] = {"Model": "Decision Tree", "RMSE": dt_evaluator_rmse.evaluate(dt_predictions), "R2": dt_evaluator.evaluate(dt_predictions)}

print("RMSE on validation data : %g" % dt_evaluator_rmse.evaluate(dt_predictions))
print("r2 on validation data : %g" % dt_evaluator.evaluate(dt_predictions))


# In[60]:


# predictions = lr_model.transform(test_data)
# predictions.select("predicted_pH","pH","features").show()


# ### Random Forest

# In[61]:


from pyspark.ml.regression import RandomForestRegressor
rf = RandomForestRegressor(featuresCol = 'scaled_features', labelCol = 'pH', predictionCol = "predicted_pH")
rf_model = rf.fit(training_data)


# In[62]:


rf_predictions = rf_model.transform(validation_data)

rf_evaluator_rmse = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "rmse")
rf_evaluator = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "r2")

ml_df.loc[len(ml_df)] = {"Model": "Random Forest", "RMSE": rf_evaluator_rmse.evaluate(rf_predictions), "R2": rf_evaluator.evaluate(rf_predictions)}

print("RMSE on validation data : %g" % rf_evaluator_rmse.evaluate(rf_predictions))
print("r2 on validation data : %g" % rf_evaluator.evaluate(rf_predictions))


# ### Gradient Boosted Tree Regression

# In[63]:


from pyspark.ml.regression import GBTRegressor

# Define GBTRegressor model
gbt = GBTRegressor(featuresCol = 'scaled_features', labelCol = 'pH', predictionCol = "predicted_pH")

# Fit the model
gbt_model = gbt.fit(training_data)


# In[64]:


gbt_predictions = gbt_model.transform(validation_data)

gbt_evaluator_rmse = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "rmse")
gbt_evaluator = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "r2")

ml_df.loc[len(ml_df)] = {"Model": "Gradient Boosted Tree Regression", "RMSE": gbt_evaluator_rmse.evaluate(gbt_predictions), "R2": gbt_evaluator.evaluate(gbt_predictions)}

print("RMSE on validation data : %g" % gbt_evaluator_rmse.evaluate(gbt_predictions))
print("r2 on validation data : %g" % gbt_evaluator.evaluate(gbt_predictions))


# In[65]:


output_path = "/storage/home/tah5808/DS410 Project/ML_Models.csv"
ml_df.to_csv(output_path)


# ### Hyperparameter Tuning

# In[66]:


from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
dt = DecisionTreeRegressor(featuresCol = "scaled_features", labelCol = "pH", predictionCol = "predicted_pH")

paramGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [1, 2, 3, 4, 5, 6])  # max depth of the tree
             .addGrid(dt.minInstancesPerNode, [1, 2, 3, 4, 5, 6, 7, 8])  # minimum number of instances each child must have
             .build())

evaluator = RegressionEvaluator(predictionCol = "predicted_pH", labelCol = "pH", metricName = "rmse")
evaluator_r2 = RegressionEvaluator(predictionCol = "predicted_pH", labelCol = "pH", metricName = "r2")

crossval = CrossValidator(estimator = dt,
                          estimatorParamMaps = paramGrid,
                          evaluator = evaluator,
                          numFolds = 3)

cvModel = crossval.fit(training_data)

best_model = cvModel.bestModel

best_model.save('./Best_DT')

predictions = best_model.transform(validation_data)
rmse = evaluator.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print("RMSE on validation data : %g" % rmse)
print("r2 on validation data : %g" % r2)


# In[67]:


params = [{p.name: v for p, v in m.items()} for m in cvModel.getEstimatorParamMaps()]

dt_hpt = pd.DataFrame.from_dict([
    {cvModel.getEvaluator().getMetricName(): metric, **ps} 
    for ps, metric in zip(params, cvModel.avgMetrics)
])


# In[68]:


output_path = "/storage/home/tah5808/DS410 Project/DT_HPT_Local.csv"
dt_hpt.to_csv(output_path)


# In[69]:


#print(best_model.getMaxDepth())
#print(best_model.getMinInstancesPerNode())


# In[70]:


#for param, value in best_model.extractParamMap().items():
#    print(f"{param.name}: {value}")


# ## Testing

# In[71]:


final_df = pd.DataFrame(columns = ["Model", "RMSE", "R2"])


# In[72]:


# Training
dt = DecisionTreeRegressor(featuresCol = "scaled_features", labelCol = "pH",                            predictionCol = "predicted_pH", maxDepth = best_model.getMaxDepth(),                            minInstancesPerNode = best_model.getMinInstancesPerNode())
dt_model = dt.fit(training_data)


# In[73]:


# Testing
dt_predictions = dt_model.transform(test_data)

dt_evaluator_rmse = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "rmse")
dt_evaluator = RegressionEvaluator(predictionCol = "predicted_pH",                  labelCol = "pH", metricName = "r2")

final_df.loc[len(final_df)] = {"Model": "Decision Tree", "RMSE": dt_evaluator_rmse.evaluate(dt_predictions), "R2": dt_evaluator.evaluate(dt_predictions)}

print("RMSE on test data : %g" % dt_evaluator_rmse.evaluate(dt_predictions))
print("r2 on test data : %g" % dt_evaluator.evaluate(dt_predictions))


# In[74]:


output_path = "/storage/home/tah5808/DS410 Project/Final_Model.csv"
final_df.to_csv(output_path)

