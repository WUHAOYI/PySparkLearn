import heapq

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("TopN").getOrCreate()
sc = spark.sparkContext

# 数据格式: 类别,产品名称,销量
file_path = "/spark-data/data/cases/sales_data.txt"
rdd = sc.textFile(file_path).map(lambda x: x.split(","))
rdd = rdd.map(lambda x: (x[0], x[1], int(x[2])))

# 问题 求销量最高的Top5 (全局 or 分组)

# 1. RDD 全局 TopN
# 思路 利用takeOrdered方法 进行降序排序
# res1 = rdd.takeOrdered(5,lambda x: -x[2])
# print(res1) # [('Food', 'Coca-Cola', 7500), ('Food', 'Lays Chips', 7300), ('Food', 'Nestle Chocolate', 7200), ('Food', 'Red Bull', 7100), ('Food', "Kellogg's Cereal", 7000)]

# 2. RDD 分组 TopN (依据产品类别分组)

# 方法1 先计算出所有的key来，每次取出一个key中的数据进行过滤
# categories = rdd.map(lambda x: x[0]).distinct().collect()
# all_items = []
# for cat in categories:
#     item = rdd.filter(lambda x: x[0] == cat).takeOrdered(5, lambda x: -x[2])
#     all_items.append((cat, item))
# for cat,items in all_items:
#     print(f"类别: {cat}")
#     for item in items:
#         print(f"\t{item[1]}\t {item[2]}")

# 方法2 先groupByKey 然后利用mapValues进行分组
# res2_2 = rdd.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(lambda x : sorted(x,key=lambda x : -x[1])[:5]).collect()
# print(res2_2)

# 方法3 自定义分区器 然后在每个分区内进行sort操作
# categories = rdd.map(lambda x: x[0]).distinct().collect()
# categories_dict = {cat: idx for idx, cat in enumerate(categories)}
# categories_num = len(categories)
#
#
# def partitionFunc(k):
#     return categories_dict.get(k, -1)
#
#
# partitioned_rdd = rdd.map(lambda x: (x[0], (x[1], x[2]))).partitionBy(categories_num, partitionFunc)
#
#
# def get_partitioned_top5_sort(iter):
#     return sorted(iter, key=lambda x: x[1][1], reverse=True)[:5]
#
# # 可以用heap sort优化大量数据的排序 时间复杂度由O(nlogn)下降到O(nlogk)
# def get_partitioned_top5_heapq(iter):
#     return heapq.nlargest(5, iter, key=lambda x: x[1][1])
#
#
# res3 = partitioned_rdd.mapPartitions(get_partitioned_top5_heapq).collect()
# print(res3)

# df schema定义
schema = StructType().add("category", StringType(), nullable=False) \
    .add("name", StringType(), nullable=False) \
    .add("score", IntegerType(), nullable=False)
df = spark.createDataFrame(rdd, schema=schema)

# 3. DF 全局 TopN
# df.orderBy(df['score'], ascending=False).limit(5).show()

# 4. df 分组 TopN
# df.withColumn("rank", rank().over(Window.partitionBy(df['category']).orderBy(col('score').desc()))) \
#     .filter(col("rank") <= 5) \
#     .select(col("category"), col("name"), col("score")) \
#     .show()
