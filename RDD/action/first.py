# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：取出rdd的第一个元素

if __name__ == '__main__':
    conf = SparkConf().setAppName("first").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    result = rdd.first()
    print(result)