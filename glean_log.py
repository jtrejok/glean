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
from collections import defaultdict, Counter
import uuid
import pandas as pd
import math
from datetime import date, datetime, timedelta

from pyspark.sql.functions import (
    udf, array, struct, max, split,
    lit, first, last, countDistinct,
    date_trunc, sum, avg, explode_outer)

import sys

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
line_item_df = spark.read.format("csv")\
    .option("Header", True)\
    .schema(line_item_schema)\
    .load(line_item_file)

line_item_df.printSchema()
line_item_df.show(3)


def get_vender_not_seen_gleans(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_dates = cur_row["Invoice Dates"]
    invoice_id = "INVOICE_ID"
    gleans = []
    if len(invoice_dates) <= 1:
        return gleans

    for i in range(1, len(invoice_dates)):
        # if not seen in 90 days
        last_date = invoice_dates[i-1]
        cur_date = invoice_dates[i]
        count_of_months_since_last_invoice = cur_date - last_date
        glean_id = str(uuid.uuid4())
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

    for i in range(1, len(invoice_dates)):
        # if not seen in 90 days
        last_date = invoice_dates[i-1]
        cur_date = invoice_dates[i]
        invoice_id = invoice_ids[i]
        count_of_months_since_last_invoice = cur_date - last_date
        glean_id = str(uuid.uuid4())
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


vendor_glean_2 = udf(
    lambda row: get_vender_not_seen_gleans_2(row),
    ArrayType(StringType())
)


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

    new_dates = grouped_dates.withColumn("gleans", vendor_glean_2(
        struct(
            [grouped_dates["canonical_vendor_id"], grouped_dates["Invoice Dates"], grouped_dates["sorted_invoices"]
             ])))

    # show only the vendor id and gleans
    id_and_gleans = new_dates.select("canonical_vendor_id", "gleans")
    id_and_gleans.show()

    # get row for each glean
    id_and_gleans = id_and_gleans.select(
        "canonical_vendor_id", explode_outer("gleans").alias("gleans"))
    id_and_gleans.show()

    return id_and_gleans


vendor_gleans = vendor_take_2()


def get_accrual_alert_invoice(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]

    invoice_id = cur_row["invoice_id"]

    try:
        period_end = cur_row["period_end_date"]
        invoice_date = cur_row["invoice_date"]
        time_dif = period_end - invoice_date
    except:
        return ""

    if time_dif.days > 90:
        glean_id = str(uuid.uuid4())
        cur_date = invoice_date
        text = (f"Line items from vendor {canonical_vendor_id}"
                f" in this invoice cover future periods (through {period_end})")
        return (f"{glean_id}**{cur_date}**{text}**accrual_alert**"
                f"INVOICE**{invoice_id}**{canonical_vendor_id}"
                )
    else:
        return ""


def get_accrual_alert_line(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_id = cur_row["invoice_id"]

    try:
        invoice_date = datetime.strptime(
            cur_row["invoice_date"], '%Y-%m-%d').date()
        period_end = cur_row["period_end_date"][-1]
        time_dif = period_end - invoice_date
    except:
        # no dates found
        return ""

    if time_dif.days > 90:
        glean_id = str(uuid.uuid4())
        cur_date = invoice_date
        text = (f"Line items from vendor {canonical_vendor_id}"
                f" in this invoice cover future periods (through {period_end})")
        return (f"{glean_id}**{cur_date}**{text}**accrual_alert**"
                f"INVOICE**{invoice_id}**{canonical_vendor_id}"
                )
    else:
        return ""


accrual_glean = udf(
    lambda row: get_accrual_alert_invoice(row),
    StringType()
)

accrual_glean_line = udf(
    lambda row: get_accrual_alert_line(row),
    StringType()
)


def accrual_alert():
    # add invoice_date and canonical vendor id cols to line item df
    line_item_with_date = line_item_df.withColumn(
        "invoice_date", lit(None).cast(StringType()))\
        .withColumn("canonical_vendor_id", lit(None).cast(StringType()))

    # union the invoice df and line item df using specified cols
    cols_to_use = ["invoice_id", "invoice_date",
                   "period_end_date", "canonical_vendor_id"]
    unioned_df_grouped = invoice_df.select(*cols_to_use)\
        .unionAll(line_item_with_date.select(*cols_to_use))\
        .groupBy("invoice_id")

    # collect all the period end dates for each invoicee_id
    dates_grouped = unioned_df_grouped.agg(
        sort_array(collect_list("period_end_date")).alias("period_end_date"),
        first("invoice_date", ignorenulls=True).alias(
            "invoice_date"),  # should be only single invoice date
        first("canonical_vendor_id", ignorenulls=True).alias(
            "canonical_vendor_id")  # there should only be one canonical vendor id
    )

    invoice_and_line_alerts = dates_grouped.withColumn(
        "gleans",
        accrual_glean_line(
            struct([
                dates_grouped["canonical_vendor_id"],
                dates_grouped["invoice_date"],
                dates_grouped["period_end_date"],
                dates_grouped["invoice_id"]
            ]))
    )
    invoice_and_line_alerts.show()

    ids_and_gleans = invoice_and_line_alerts.select(
        "canonical_vendor_id", "invoice_id", "gleans")
    ids_and_gleans = ids_and_gleans.filter(ids_and_gleans.gleans != "")
    ids_and_gleans.show()

    return ids_and_gleans

# accrual_alert()


def get_month_glean(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    total_amount = cur_row["total_amount"]
    invoice_id = cur_row["invoice_id"]
    try:
        average = cur_row["average"]
        month = cur_row["month"]
    except:
        return ""

    percentage = (total_amount-average)/average
    glean_id = str(uuid.uuid4())
    cur_date = month
    text = ""

    if total_amount < 100:
        return ""
    elif total_amount < 1000:
        if percentage >= 5.0:
            text = (f"Monthly spend with {canonical_vendor_id} is "
                    f"{total_amount-average} ({percentage}%) higher than average")
    elif total_amount < 10000:
        if percentage >= 2.0:
            text = (f"Monthly spend with {canonical_vendor_id} is "
                    f"{total_amount-average} ({percentage}%) higher than average")

    elif total_amount >= 10000:
        if percentage >= 0.5:
            text = (f"Monthly spend with {canonical_vendor_id} is "
                    f"{total_amount-average} ({percentage}%) higher than average")
    if text:
        return (f"{glean_id}**{cur_date}**{text}**large_month_increase_mtd**"
                f"VENDOR**{invoice_id}**{canonical_vendor_id}")
    return ""


month_alert_glean = udf(
    lambda row: get_month_glean(row),
    StringType()
)


def large_month_increase():
    # group by vendor and compute total for months
    w = Window.partitionBy('canonical_vendor_id').orderBy('invoice_date')

    dates_sorted = invoice_df.withColumn(
        'sorted_invoices', collect_list("invoice_id").over(w)
    )
    months_sum = invoice_df.groupBy(
        'canonical_vendor_id',
        date_trunc("month", invoice_df.invoice_date).alias('month'))\
        .agg(sum("total_amount").alias('total_amount'), last('invoice_id').alias('invoice_id'))

    months_sum.show()

    # get dataframe of averages for each vendor

    # combine with dataframe of total monthly costs and compare
    avgs = invoice_df.groupBy(
        'canonical_vendor_id')\
        .agg(avg("total_amount").alias("average"))

    avgs.show()

    months_and_avgs = months_sum.join(avgs, 'canonical_vendor_id')
    months_and_avgs.show()

    month_gleans = months_and_avgs.withColumn(
        "gleans",
        month_alert_glean(
            struct([
                months_and_avgs['canonical_vendor_id'],
                months_and_avgs['average'],
                months_and_avgs['total_amount'],
                months_and_avgs["month"],
                months_and_avgs["invoice_id"]

            ])
        )
    )
    month_gleans = month_gleans.filter(month_gleans.gleans != "")

    month_gleans.show()

    return month_gleans

# large_month_increase()


def get_day(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_dates = cur_row["Invoice Dates"]

    # get count of days and return most common one
    # map dates to just days
    # sorted by date
    days = [i.day for i in invoice_dates]
    counts = Counter(days)
    most_common = sorted(counts.most_common(), key=lambda x: x[0])

    if most_common:
        return most_common[0][0]
    else:
        return None


def get_basis(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_dates = cur_row["Invoice Dates"]

    # sorted by date
    months = [i.month for i in invoice_dates]
    # get differences in months
    month_diffs = [(j-i) % 12 for i, j in zip(months[:-1], months[1:])]

    if len(month_diffs) < 1:
        return ""

    avg_diff = math.fsum(month_diffs)/float(len(month_diffs))

    if avg_diff >= 2:
        return "QUARTERLY"

    return "MONTHLY"


def get_glean_dates(month_diffs, month_delta, invoice_dates, cur_year, cur_month, common_day, month_index):
    # see if these are consecutive months
    glean_dates = []
    consecutive = False
    for month_i, diff in enumerate(month_diffs):
        if month_i <= 1:
            if diff == month_delta:
                consecutive = True
        else:
            if consecutive:
                if diff >= 2:
                    glean_date = date(cur_year, cur_month, common_day)
                    glean_dates.append(glean_date)
                    # trigger until next date
                    if month_index != len(invoice_dates)-1:
                        next_date = invoice_dates[month_index+1]
                        while glean_date < next_date or glean_date.month == cur_month:
                            glean_date = glean_date + timedelta(days=1)
                            glean_dates.append(
                                glean_date
                            )
    return glean_dates


def get_no_invoice_glean(cur_row):
    canonical_vendor_id = cur_row["canonical_vendor_id"]
    invoice_dates = cur_row["Invoice Dates"]
    invoice_ids = cur_row["sorted_invoices"]
    common_day = cur_row["common_day"]
    basis = cur_row["basis"]

    gleans = []
    if len(invoice_dates) <= 3:
        return gleans

    glean_dates = []
    for i in range(3, len(invoice_dates)):
        cur_month = invoice_dates[i].month
        cur_year = invoice_dates[i].year
        four_months = [i.month for i in invoice_dates[i-3:i+1]]
        cur_invoice = invoice_ids[i]

        month_diffs = [
            (j-i) for i, j in zip(four_months[:-1], four_months[1:])]
        if basis == "MONTHLY":
            glean_tuple = (cur_invoice, get_glean_dates(
                month_diffs, 1, invoice_dates, cur_year, cur_month, common_day, i))
            glean_dates.append(glean_tuple)

        else:
            glean_tuple = (cur_invoice, get_glean_dates(
                month_diffs, 3, invoice_dates, cur_year, cur_month, common_day, i))
            glean_dates.append(glean_tuple)

    glean_strings = []
    for in_id, g_dates in glean_dates:
        for g in g_dates:
            glean_id = str(uuid.uuid4())
            text = (f"{canonical_vendor_id} generally charges between on "
                    f"{common_day} day of each month invoices "
                    f"are sent. On {g}, an invoice from "
                    f"{canonical_vendor_id} has not been received")

            glean_string = (f"{glean_id}**{g}**{text}**no_invoice_received**"
                            f"VENDOR**{in_id}**{canonical_vendor_id}")
            glean_strings.append(glean_string)

    return glean_strings


get_common_day = udf(
    lambda row: get_day(row),
    IntegerType()
)
get_time_basis = udf(
    lambda row: get_basis(row),
    StringType()
)

no_invoice_glean = udf(
    lambda row: get_no_invoice_glean(row),
    ArrayType(StringType())
)


def no_invoice_received():
    # group by dates
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

    # get most frequent day per vendor
    most_common_days = grouped_dates.withColumn(
        "common_day",
        get_common_day(
            struct([
                grouped_dates["canonical_vendor_id"],
                grouped_dates["Invoice Dates"],
            ])
        )
    )
    most_common_days.show()

    # determine if vendor is monthly or quarterly
    time_basis = grouped_dates.withColumn(
        "basis",
        get_time_basis(
            struct([
                grouped_dates["canonical_vendor_id"],
                grouped_dates["Invoice Dates"],
            ])
        )
    )
    # remove one off vendors
    time_basis = time_basis.filter(time_basis.basis != "")
    time_basis.show()

    # go through vendor and invoice dates and compute based on months as
    # in not seen in a while using info of monthly or quarterly and most
    # frequent date

    day_basis_invoices = most_common_days.join(
        time_basis.select("canonical_vendor_id", "basis"),
        "canonical_vendor_id"
    )

    day_basis_invoices.show()

    invoice_gleans = day_basis_invoices.withColumn(
        "gleans",
        no_invoice_glean(
            struct([
                day_basis_invoices["canonical_vendor_id"],
                day_basis_invoices["basis"],
                day_basis_invoices["Invoice Dates"],
                day_basis_invoices["sorted_invoices"],
                day_basis_invoices["common_day"],
            ])
        )
    )

    id_and_gleans = invoice_gleans.select("canonical_vendor_id", "gleans")

    id_and_gleans.show()

    # get row for each glean
    id_and_gleans = id_and_gleans.select(
        "canonical_vendor_id", explode_outer("gleans").alias("gleans"))

    # remove null
    id_and_gleans.show()
    id_and_gleans = id_and_gleans.filter(id_and_gleans.gleans.isNotNull())
    id_and_gleans.show()

    return id_and_gleans


#test_df = no_invoice_received()


def split_dataframe(input_df):
    split_col = split(input_df["gleans"], "[**]")
    input_df = input_df.withColumn("glean_id", split_col.getItem(0))
    input_df = input_df.withColumn("glean_date", split_col.getItem(2))
    input_df = input_df.withColumn("glean_text", split_col.getItem(4))
    input_df = input_df.withColumn("glean_type", split_col.getItem(6))
    input_df = input_df.withColumn("glean_location", split_col.getItem(8))
    input_df = input_df.withColumn("invoice_id", split_col.getItem(10))
    #input_df = input_df.withColumn("canonical_vendor_id", split_col.getItem(6))
    input_df.show()
    return input_df

# split_dataframe(test_df)


if __name__ == '__main__':
    vendor_gleans = vendor_take_2()
    accrual_gleans = accrual_alert()
    month_gleans = large_month_increase()
    no_invoice_gleans = no_invoice_received()

    cols_to_union = ["canonical_vendor_id", "gleans"]
    unioned_df = vendor_gleans.unionAll(
        accrual_gleans.select(*cols_to_union))\
        .unionAll(month_gleans.select(*cols_to_union))\
        .unionAll(no_invoice_gleans.select(*cols_to_union))

    # remove null values
    vendor_id_gleans = unioned_df.filter(unioned_df.gleans.isNotNull())
    vendor_id_gleans = split_dataframe(vendor_id_gleans)
    vendor_id_gleans = vendor_id_gleans.drop("gleans")
    vendor_id_gleans.show(1000)

    vendor_id_gleans.toPandas().to_csv("gleans.csv")
    spark.stop()


spark.stop()
