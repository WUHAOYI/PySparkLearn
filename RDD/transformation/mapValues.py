# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：针对KV型rdd，对其values进行map操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("mapValues").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([(1,1), (2,2), (3,3), (4,4), (5,5)])
    result = rdd.mapValues(lambda x: x * 10)
    print(result.collect())