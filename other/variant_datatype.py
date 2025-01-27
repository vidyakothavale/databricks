# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create a table with a VARIANT column
# MAGIC CREATE or replace TABLE variant_table (
# MAGIC     id INT,
# MAGIC     json_column VARIANT
# MAGIC );
# MAGIC  
# MAGIC -- Insert some data by parsing JSON strings
# MAGIC INSERT INTO variant_table
# MAGIC
# MAGIC VALUES
# MAGIC
# MAGIC (1, PARSE_JSON('{"name": "John", "age": 30, "city": "New York"}')),
# MAGIC
# MAGIC (2, PARSE_JSON('{"name": "Jane", "age": 25, "city": "Los Angeles"}')),
# MAGIC
# MAGIC (3, PARSE_JSON('{"name": "Mike", "age": 35, "city": "Chicago"}')),
# MAGIC
# MAGIC (4, PARSE_JSON('{"name": "Emily", "age": 28, "city": "San Francisco"}'));
# MAGIC
# MAGIC
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT typeof(json_column
# MAGIC ) AS column_type
# MAGIC FROM variant_table
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dvltest01.default.store_data AS
# MAGIC SELECT parse_json(
# MAGIC   '{
# MAGIC     "store":{
# MAGIC         "fruit": [
# MAGIC           {"weight":8,"type":"apple"},
# MAGIC           {"weight":9,"type":"pear"}
# MAGIC         ],
# MAGIC         "basket":[
# MAGIC           [1,2,{"b":"y","a":"x"}],
# MAGIC           [3,4],
# MAGIC           [5,6]
# MAGIC         ],
# MAGIC         "book":[
# MAGIC           {
# MAGIC             "author":"Nigel Rees",
# MAGIC             "title":"Sayings of the Century",
# MAGIC             "category":"reference",
# MAGIC             "price":8.95
# MAGIC           },
# MAGIC           {
# MAGIC             "author":"Herman Melville",
# MAGIC             "title":"Moby Dick",
# MAGIC             "category":"fiction",
# MAGIC             "price":8.99,
# MAGIC             "isbn":"0-553-21311-3"
# MAGIC           },
# MAGIC           {
# MAGIC             "author":"J. R. R. Tolkien",
# MAGIC             "title":"The Lord of the Rings",
# MAGIC             "category":"fiction",
# MAGIC             "reader":[
# MAGIC               {"age":25,"name":"bob"},
# MAGIC               {"age":26,"name":"jack"}
# MAGIC             ],
# MAGIC             "price":22.99,
# MAGIC             "isbn":"0-395-19395-8"
# MAGIC           }
# MAGIC         ],
# MAGIC         "bicycle":{
# MAGIC           "price":19.95,
# MAGIC           "color":"red"
# MAGIC         }
# MAGIC       },
# MAGIC       "owner":"amy",
# MAGIC       "zip code":"94025",
# MAGIC       "fb:testid":"1234"
# MAGIC   }'
# MAGIC ) as raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM store_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw:owner FROM store_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use backticks to escape special characters.
# MAGIC SELECT raw:`zip code`, raw:`fb:testid` FROM store_data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Extract variant nested fields

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT raw:store.bicycle FROM store_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use brackets
# MAGIC SELECT raw:store['bicycle'] FROM store_data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Extract values from variant arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Index elements
# MAGIC SELECT raw:store.fruit[0], raw:store.fruit[1] FROM store_data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Flatten variant objects and arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT key, value
# MAGIC   FROM store_data,
# MAGIC   LATERAL variant_explode(store_data.raw:store);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pos, value
# MAGIC   FROM store_data,
# MAGIC   LATERAL variant_explode(store_data.raw:store.basket[0]);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Variant type casting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- price is returned as a double, not a string
# MAGIC SELECT raw:store.bicycle.price::double FROM store_data
# MAGIC

# COMMAND ----------

print("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cast into more complex types
# MAGIC SELECT cast(raw:store.bicycle AS STRUCT<price DOUBLE, color STRING>) bicycle FROM store_data;
# MAGIC -- `::` also supported
# MAGIC SELECT raw:store.bicycle::STRUCT<price DOUBLE, color STRING> bicycle FROM store_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   is_variant_null(parse_json(NULL)) AS sql_null,
# MAGIC   is_variant_null(parse_json('null')) AS variant_null,
# MAGIC   is_variant_null(parse_json('{ "field_a": null }'):field_a) AS variant_null_value,
# MAGIC   is_variant_null(parse_json('{ "field_a": null }'):missing) AS missing_sql_value_null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table json_variant

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE json_variant (variant_column VARIANT);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO json_variant (variant_column)
# MAGIC SELECT parse_json(data) FROM json.`dbfs:/FileStore/state.json`;
