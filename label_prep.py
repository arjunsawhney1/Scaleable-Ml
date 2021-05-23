from pyspark.sql import SparkSession


if __name__ == '__main__':
    qtr = '2019Q1'
    fname = 'data/historical_data_' + qtr + '/historical_data_time_' + qtr + '.txt'
    spark = SparkSession.builder.appName('rajeev-test').getOrCreate()
    df = spark.read.load(fname, format = "csv", sep="|", inferSchema="true", header="false")
    df.show()

    drop_cols = []
    for i in range(30):
        if i not in [0, 3, 8]:
            drop_cols.append("_c" + str(i))

    df = df.drop(drop_cols)