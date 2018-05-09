# Databricks notebook source
# MAGIC %md
# MAGIC #### [Specifications](https://jira.marketshare.com/confluence/display/CPUSAA/OUTCOME)
# MAGIC ##### Project: USAA
# MAGIC ##### Description: Outcome
# MAGIC Author: Yifei Wang
# MAGIC ###### Release History
# MAGIC * 2018-05-09 Enhancing 101
# MAGIC * 2018-05-09 Jira 101 - Inital version for QA
# MAGIC * 2018-05-09 Initail version

# COMMAND ----------

from pyspark.sql.functions import *
from dmutil import *

# COMMAND ----------

align_date(align_to="FR", align_BeginEnd="E")

# COMMAND ----------

dbutils.widgets.text('from_date','2013-02-02')
dbutils.widgets.text('to_date','2018-04-28')
dbutils.widgets.text('path_prefix','/mnt/s3/prod')
dbutils.widgets.text('in_path','%s/usaa_strategy/2017q4_rebuild/outcome/scm/')
dbutils.widgets.text('lkp_path','%s/usaa_strategy/2017q4_rebuild/lkp/%s')
dbutils.widgets.text('out_path','%s/usaa_strategy/2017q4_rebuild/out/trn_o.csv')

# COMMAND ----------

from_date = dbutils.widgets.get("from_date")
to_date = dbutils.widgets.get("to_date")
path_prefix = dbutils.widgets.get("path_prefix")

in_f1 = dbutils.widgets.get("in_path") % path_prefix
in_lkp = dbutils.widgets.get("lkp_path") % (path_prefix,"map_o_prd.csv")
out_file = dbutils.widgets.get("out_path") % path_prefix

# COMMAND ----------

df_in = sqlContext.read.format('csv').options(header='true').load(in_f1).selectExpr(
  "align_date(`WEEK_DATE`) as X_DT",
  "ESF_CHANNEL_DC as esf_chn",
  "upper(PROD) as PROD",
  "SOR_DERIVED as X_SEG",
  "DMA_DERIVED as X_GEO",
  "stack(6,'O_APPS_CNT',APS,'O_QUOTEC_CNT',QTC,'O_QUOTES_CNT',QTS,'O_APPC_CNT',APC,'W_SESSIONS',SS,'O_SAL_CNT',SALES) as (metric,value)"
)

df_lkp = sqlContext.read.format('csv').options(header='true',multiline='true').load(in_lkp)

df_in.createOrReplaceTempView('df')
df_lkp.createOrReplaceTempView('lkp_prd')

# COMMAND ----------

#sum up values by variable names
df_all=sql("""
--Outcome Applications Completed/Started Count and Outcome Quotes Completed/Started Count
select 
  X_DT,
    X_GEO,
    X_SEG,
    X_PRD,
    metric,
    sum(value) as value
from 
  (select
    X_DT,
    X_GEO,
    X_SEG,
    Product as X_PRD,
    metric,
    case
      when Product = "HI" and X_DT >= "2015-09-18" and X_DT <= "2015-10-23" and metric in ("O_QUOTES_CNT","O_APPC_CNT") then 0
      when Product = "LI" and X_DT >= "2016-01-22" and X_DT <= "2016-02-12" and metric = "O_SAL_CNT" then 0
      when X_DT= "2015-10-02" and metric in ("O_QUOTES_CNT","O_APPC_CNT") then 0
      else value * `Allocation Percent\r` end as value
  from df
    left join lkp_prd
  on
    df.PROD=upper(lkp_prd.PROD)
  where Product <> "DROP" and metric not in ("W_SESSIONS"))
group by 1,2,3,4,5

union 

--Store Sessions Count
select
  X_DT,
  X_GEO,
  X_SEG,
  Product as X_PRD,
  metric,
  sum(value * `Allocation Percent\r`) as value
from df
  left join lkp_prd
on
  df.PROD=upper(lkp_prd.PROD)
where Product <> "DROP" and metric = "W_SESSIONS" and esf_chn in ("INTERNET","MOBILE")
group by 1,2,3,4,5 
""")

df_all.createOrReplaceTempView('df_all')

# COMMAND ----------

#fix data for HI and LI
data_fix=sc.parallelize([("2015-09-18","2015-10-23","HI","O_QUOTES_CNT"),
                ("2015-09-18","2015-10-23","HI","O_APPC_CNT"),
                ("2016-01-22","2016-02-12","LI","O_SAL_CNT")])

# COMMAND ----------

for i in range (3):
    df_fix_sub=sql("""
    select 
        X_DT,
        X_GEO,
        X_SEG,
        Product as X_PRD,
        metric,
        sum(value*`Allocation Percent\r`) as value
    from 
      df
        left join lkp_prd
      on
        df.PROD=upper(lkp_prd.PROD)
      where X_DT>= '{0}' and X_DT <= '{1}' and Product ='{2}' and metric ='{3}'
      group by 1,2,3,4,5""".format(
    data_fix.collect()[i][0],
    data_fix.collect()[i][1],
    data_fix.collect()[i][2],
    data_fix.collect()[i][3]))
    
    fill_gap(df_fix_sub,
    ts_start = '{}'.format(data_fix.collect()[i][0]), 
    ts_end ='{}'.format(data_fix.collect()[i][1]),
    align_to='FR',align_BeginEnd='E',fill_method='linear',stack_format='S'
    ).createOrReplaceTempView('df_fix_{}'.format(i))
  

# COMMAND ----------

#allocate values to where DMA is missing (X_GEO='ZZ')
sql("""
--union df and the fixed data
with df_all_u as(
select * from df_all
union
select * from df_fix_0
union
select * from df_fix_1
union
select * from df_fix_2
),

alloc as(
select
   X_DT,
   X_SEG,
   X_GEO,
   X_PRD,
   metric,
   value/(sum(value) over (partition by  X_DT,X_SEG,X_PRD,metric)) as alloc
from df_all_u

where X_GEO <> "ZZ"
)

select 
  d.X_DT,
  case 
    when d.X_GEO = "ZZ" then a.X_GEO
    else d.X_GEO end as X_GEO,
  d.X_SEG,
  d.X_PRD,
  d.metric,
  sum(case when d.X_GEO="ZZ" then d.value * alloc else d.value end) as value
from 
  df_all_u d
  left join alloc a
on d.X_DT=a.X_DT and d.X_SEG=a.X_SEG and d.X_PRD=a.X_PRD and d.metric=a.metric and d.X_GEO="ZZ"
group by 1,2,3,4,5""").createOrReplaceTempView('df_all1')

# COMMAND ----------

#create dummies
df_out=sql("""
select 
  *
from df_all1

union

select
   X_DT,
   X_SEG,
   X_GEO,
   X_PRD,
   case 
     when metric='O_APPS_CNT' then 'D_VLD_XID_APPS'
     when metric='O_QUOTEC_CNT' then 'D_VLD_XID_QUOTES'
     when metric='O_QUOTES_CNT' then 'D_ZERO_XID_QUOTES'
     when metric='O_APPC_CNT' then 'D_ZERO_XID_APPC'
     when metric='W_SESSIONS' then 'D_VLD_XID_W_SESSIONS'
     when metric='O_SAL_CNT' then 'D_ZERO_XID_SAL' end as metric,
   case when value =0 then 0 else 1 end as value
from df_all1
""")

# COMMAND ----------

df_out = fill_gap(df_out.where('value is not null'),ts_start=from_date, ts_end=to_date, align_to='FR', align_BeginEnd='E', fill_method='zero')

# COMMAND ----------

write_output(df_out,out_file,dbutils)