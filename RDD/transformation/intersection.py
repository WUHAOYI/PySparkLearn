# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：求两个rdd的交集

if __name__ == '__main__':
    conf = SparkConf().setAppName("intersection").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('a', 3)])
    rdd2 = sc.parallelize([('a', 1), ('b', 3)])
    result = rdd1.intersection(rdd2)
    print(result.collect())