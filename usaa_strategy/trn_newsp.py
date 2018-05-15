# Databricks notebook source
# MAGIC %md
# MAGIC #### [Specifications](https://jira.marketshare.com/browse/DMBOSEUS-482)
# MAGIC ##### Project: XYZ Project
# MAGIC ##### Description: Variable Group
# MAGIC Author: Your Name
# MAGIC ###### Release History
# MAGIC * 2018-05-15 Jira 101 - Release to QA
# MAGIC * 2018-02-21 Bug Fix 101
# MAGIC * 2018-02-20 Initial Version

# COMMAND ----------

from pyspark.sql.functions import *
from dmutil import *

# COMMAND ----------

align_date(align_to="SU", align_BeginEnd="B")

# COMMAND ----------

dbutils.widgets.text('from_date','1900-01-01')
dbutils.widgets.text('to_date','1900-01-01')
dbutils.widgets.text('path_prefix','/mnt/s3/prod')
dbutils.widgets.text('in_path','%s/<client_project>/<phase>/<variablegroup>/<raw|scm|src>/')
dbutils.widgets.text('lkp_path','%s/<client_project>/<phase>/lkp/%s')
dbutils.widgets.text('out_path','%s/<client_project>/<phase>/out/%s')

# COMMAND ----------

from_date = dbutils.widgets.get("from_date")
to_date = dbutils.widgets.get("to_date")
path_prefix = dbutils.widgets.get("path_prefix")

in_f1 = dbutils.widgets.get("in_path") % path_prefix
in_lkp = dbutils.widgets.get("lkp_path") % (path_prefix,"dimension.csv")
out_file1 = dbutils.widgets.get("out_path") % (path_prefix, "trn_dmtemplate1.csv")
out_file2 = dbutils.widgets.get("out_path") % (path_prefix, "trn_dmtemplate2.csv")

# COMMAND ----------

df_in = sqlContext.read.format('csv').options(header='true').load(in_f1)
df_lkp = sqlContext.read.format('csv').options(header='true').load(in_lkp)

df_in.createOrReplaceTempView('<vg>')
df_lkp.createOrReplaceTempView('<lkp>')

# COMMAND ----------

df_out = sql(
"""
  <sql>
"""
)

# COMMAND ----------

df_out = fill_gap(to_skinny(df_out),ts_start=from_date, ts_end=to_date, align_to='SU', align_BeginEnd='B', fill_method='zero')

# COMMAND ----------

chk = checksum(df_out, df_in, metric=['M1','M2'], metric_in=['m1+m2+m3','m4'], metric_raw=['IMPRESSION','SPEND']).collect()
if chk:
  raise ValueError("Checksum failed %s" % chk)
print("Checksum Passed")

# COMMAND ----------

write_output(df_out,out_file,dbutils)