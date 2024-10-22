# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：针对KV型的rdd，自动按照key进行分组，然后按照聚合逻辑完成组内数据（value）的聚合

if __name__ == '__main__':
    conf = SparkConf().setAppName("reduceByKey").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('a', 1)])
    # 以key为单位进行计数
    result = rdd.reduceByKey(lambda x,y: x+y)
    print(result.collect())