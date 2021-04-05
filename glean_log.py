# import pyspark.sql.functions.sort_array
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, sort_array, pandas_udf, transform
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType, DoubleType, BooleanType, DateType,
    StructType, StructField, StringType, IntegerType, MapType
)
import uuid
import pandas as pd

from datetime import date

from pyspark.sql.functions import udf, array, struct, max
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName("Glean").getOrCreate()
invoice_schema = StructType() \
    .add("invoice_id", StringType(), True) \
    .add("invoice_date", DateType(), True) \
    .add("due_date", StringType(), True) \
    .add("period_start_date", DateType(), True) \
    .add("period_end_date", DateType(), True) \
    .add("total_amount", DoubleType(), True) \
    .add("canonical_vendor_id", StringType(), True) \


line_item_schema = StructType() \
    .add("invoice_id", StringType(), True) \
    .add("line_item_id", StringType(), True) \
    .add("period_start_date", DateType(), True) \
    .add("period_end_date", DateType(), True) \
    .add("total_amount", DoubleType(), True) \
    .add("canonical_line_item_id", StringType(), True) \

invoice_file = "data/invoice.csv"
line_item_file = "data/line_item.csv"


# load up invoice data
invoice_df = spark.read.format("csv")\
    .option("Header", True)\
    .schema(invoice_schema)\
    .load(invoice_file)

invoice_df.printSchema()
invoice_df.show(3)

# load up line item data
# line_item_df = spark.read.format("csv")\
#     .option("Header", True)\
#     .schema(line_item_schema)\
#     .load(line_item_file)

# line_item_df.printSchema()
# line_item_df.show(3)


def diff_dates(date1, date2):
    return abs(date2-date1).days

# assumes list of sorted dates
# @pandas_udf("string")

def get_vender_not_seen_gleans(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_dates = cur_row["Invoice Dates"]
    invoice_id = "INVOICE_ID"
    gleans = []
    if len(invoice_dates) <= 1:
        return gleans

    # print('the invoice dates: ', invoice_dates)
    # print('the vendor id: ', canonical_vendor_id)
    for i in range(1, len(invoice_dates)):
        # if not seen in 90 days
        last_date = invoice_dates[i-1]
        cur_date = invoice_dates[i]
        count_of_months_since_last_invoice = cur_date - last_date
        glean_id=str(uuid.uuid4())
        if count_of_months_since_last_invoice.days >= 90:
            months = abs(count_of_months_since_last_invoice.days)/30
            text = (f"First new bill in {months} "
                    f"months from vendor {canonical_vendor_id}")
            gleans.append(
                (f"{glean_id}**{cur_date}**{text}**vendor_not_seen_in_a_while**"
                 f"INVOICE**{invoice_id}**{canonical_vendor_id}"
                 )
            )
    return gleans


def get_vender_not_seen_gleans_2(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_dates = cur_row["Invoice Dates"]
    invoice_ids = cur_row["sorted_invoices"]
    gleans = []
    if len(invoice_dates) <= 1:
        return gleans

    # print('the invoice dates: ', invoice_dates)
    # print('the vendor id: ', canonical_vendor_id)
    for i in range(1, len(invoice_dates)):
        # if not seen in 90 days
        last_date = invoice_dates[i-1]
        cur_date = invoice_dates[i]
        invoice_id = invoice_ids[i]
        count_of_months_since_last_invoice = cur_date - last_date
        glean_id=str(uuid.uuid4())
        if count_of_months_since_last_invoice.days >= 90:
            months = abs(count_of_months_since_last_invoice.days)/30
            text = (f"First new bill in {months} "
                    f"months from vendor {canonical_vendor_id}")
            gleans.append(
                (f"{glean_id}**{cur_date}**{text}**vendor_not_seen_in_a_while**"
                 f"INVOICE**{invoice_id}**{canonical_vendor_id}"
                 )
            )
    return gleans


vendor_glean = udf(
    lambda row: get_vender_not_seen_gleans(row),
    ArrayType(StringType())
)

vendor_glean_2 = udf(
    lambda row: get_vender_not_seen_gleans_2(row),
    ArrayType(StringType())
)


# depends on vendor id


def vender_not_seen_in_while():
    # keep track of whether or not a vendor has been seen
    # map of vender and last seen date
    vendor_count_group = invoice_df.groupBy("canonical_vendor_id")
    print(vendor_count_group)
    print(type(vendor_count_group))
    date_sorted = vendor_count_group.agg(
        sort_array(collect_list("invoice_date")).alias("Invoice Dates")
    )

    print('date sorted: ')
    print(type(date_sorted))
    date_sorted.show()

    # go through each list and output gleans
    # date_sorted.select("canonical_vendor_id", transform)
    new_dates = date_sorted.withColumn("gleans", vendor_glean(
        struct(
            [date_sorted["canonical_vendor_id"], date_sorted["Invoice Dates"]
             ])))

    # show only the vendor id and gleans
    id_and_gleans = new_dates.select("canonical_vendor_id", "gleans")
    id_and_gleans.show()

    print(id_and_gleans.collect()[0])


def vendor_take_2():
    w = Window.partitionBy('canonical_vendor_id').orderBy('invoice_date')
    dates_sorted = invoice_df.withColumn(
        'sorted_invoices', collect_list("invoice_id").over(w)
    )
    dates_sorted.show()
    grouped_dates = dates_sorted.groupBy('canonical_vendor_id')\
        .agg(max('sorted_invoices').alias("sorted_invoices"),
             sort_array(collect_list("invoice_date")).alias("Invoice Dates")
             )

    grouped_dates.show()
    print(grouped_dates.collect()[0])

    new_dates = grouped_dates.withColumn("gleans", vendor_glean_2(
        struct(
            [grouped_dates["canonical_vendor_id"], grouped_dates["Invoice Dates"], grouped_dates["sorted_invoices"]
             ])))

    # show only the vendor id and gleans
    id_and_gleans = new_dates.select("canonical_vendor_id", "gleans")
    id_and_gleans.show()

    print(id_and_gleans.collect()[0])


vender_not_seen_in_while()
vendor_take_2()

spark.stop()


# myDf
#   .filter(col("timestamp").gt(15000))
#   .groupBy("groupingKey")
#   // collect_list and sort
#   .agg(sort_array(collect_list(struct("timestamp","aDoubleValue"))).as("data"))
#   .withColumn("data",col("data.aDoubleValue")) // project array back to aDoubleValue
