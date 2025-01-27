# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.combobox(name="FruitCB",defaultValue="apple",choices=['apple','banana','orange'],label="Fruit Combo Box")

# COMMAND ----------

dbutils.widgets.dropdown(name="FruitDD",defaultValue="apple",choices=['apple','banana','orange'],label="Fruit Dropdown")

# COMMAND ----------

dbutils.widgets.multiselect(name="FruitMS",defaultValue="apple",choices=['apple','banana','orange'],label="Fruit MultiSelect")

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text(name="FruitText",defaultValue="apple",label="Fruit Text Box")

# COMMAND ----------

dbutils.widgets.get("FruitText")

# COMMAND ----------

dbutils.widgets.get("FruitCB")

# COMMAND ----------

dbutils.widgets.get("FruitDD")

# COMMAND ----------

dbutils.widgets.get("FruitMS")

# COMMAND ----------

dbutils.widgets.getArgument("FruitMS")

# COMMAND ----------

dbutils.widgets.getArgument("FruitMs",'error:this widget is not available')

# COMMAND ----------

dbutils.widgets.remove("FruitCB")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown(name="ParaDD",defaultValue="apple",choices=['apple','banana','orange'],label="Fruit Dropdown")
dbutils.widgets.combobox(name="ParaCB",defaultValue="apple",choices=['apple','banana','orange'],label="Fruit combobox")
dbutils.widgets.multiselect(name="ParaMS",defaultValue="apple",choices=['apple','banana','orange'],label="Fruit multiselect")
dbutils.widgets.text(name="ParaT",defaultValue="apple",label="Fruit text")


# COMMAND ----------

print('combobox value is '+ dbutils.widgets.get('ParaCB'))
print('dropdown value is '+ dbutils.widgets.get('ParaDD'))
print('multiselect value is '+ dbutils.widgets.get('ParaMS'))
print('text value is '+ dbutils.widgets.get('ParaT'))

# COMMAND ----------


