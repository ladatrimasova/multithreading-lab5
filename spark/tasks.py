import calendar
from datetime import datetime
from pyspark import SparkContext, Row
from pyspark.sql import SparkSession, Window, functions

HOST_INDEX = 0
ERROR_INDEX = -2
REQUEST_INDEX = slice(5, 8)
REQUEST_METHOD_INDEX = 5
TOKENS_LENGTH = 10
DATE_INDEX = 3

def parse_interaction(line):
    try:
        tokens = line.split(" ")
        if len(tokens) != TOKENS_LENGTH:
            raise IndexError
        row = Row(**{
            "host": tokens[HOST_INDEX],
            "return_code": tokens[ERROR_INDEX],
            "request": " ".join(tokens[REQUEST_INDEX]),
            "request_method": tokens[REQUEST_METHOD_INDEX][1:],
            "date": tokens[DATE_INDEX][1:].split(":")[0]
        })
        return row

    except IndexError as e:
        return None

def convert_date(date):
    d = datetime.strptime(date, "%d/%b/%Y")
    return d.strftime("%Y-%m-%d")

sc = SparkContext()
ss = SparkSession.builder.master("local").getOrCreate()

rows = sc.textFile("/logs_nasa/NASA_access_log_Jul95").map(parse_interaction)
null_row = lambda row: row is not None
rows = rows.filter(null_row)

df = ss.createDataFrame(rows)
# task1
df_errors = df[df.return_code.like("5%")]
df_errors_length = df_errors.groupBy(["host", "request"]).count()
df_errors_length.coalesce(1).write.csv("task2.csv", mode="append")

#task2
df_requests_fields_combs = df.groupBy(["date", "request_method", "return_code"]).count()
df_requests_fields_combs.coalesce(1).write.csv("task3.csv")

#task3
days = lambda i: i * 86400
df_codes = df[(df.return_code.like("5%") | df.return_code.like("4%"))]
df_codes_grouped = df_codes.groupBy(["return_code", "date"]).count()

udf_myFunction = functions.udf(convert_date, functions.StringType())
df_codes_grouped = df_codes_grouped.withColumn("date", udf_myFunction("date"))
df_codes_grouped = df_codes_grouped.withColumn("date", df_codes_grouped.date.cast("timestamp"))


w = (Window.orderBy(functions.col("date").cast("long")).rangeBetween(-days(7), 0))
df_codes_grouped = df_codes_grouped.withColumn("rolling_average", functions.avg("count").over(w))
df_codes_grouped.coalesce(1).write.csv("task4.csv")
