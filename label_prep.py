import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit

qtr = '2019Q1'
path = 'historical_data_' + qtr + '/historical_data_time_' + qtr + '.txt'
bucket_name = "s3://ds102-team-x-scratch/"
df = spark.read.load(bucket_name + path, format = "csv", sep="|", inferSchema="true", header="false")

drop_cols = []
for i in range(30):
    if i not in [0, 3, 8]:
        drop_cols.append("_c" + str(i))

df = df.drop(*drop_cols)

df_int = df.withColumn("_c3", df["_c3"].cast(IntegerType()))
df_filt = df_int.filter((df_int._c3 > 2) | (df_int._c8 == 3) | (df_int._c8 == 6) | (df_int._c8 == 9))

df_labels = df.select('_c0').distinct()
df_default = df_filt.select('_c0').distinct()

df_default = df_default.withColumn("label", lit(1))
df_labels = df_labels.selectExpr("_c0 as seq_num")

df_labels = df_labels.join(df_default, df_labels.seq_num == df_default._c0, how='left').na.fill(0).drop('_C0')

df_labels.write.parquet("s3a://ds102-team-x-scratch/outputs/labels_" + qtr + ".parquet", mode="overwrite")