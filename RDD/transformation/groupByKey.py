# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：针对KV型rdd，按照key进行分组

if __name__ == '__main__':
    conf = SparkConf().setAppName("groupBy").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    result = rdd.groupByKey().collect() # 相当于rdd.groupBy(lambda x : x[0])
    for key,values in result:
        print(key, list(values))
