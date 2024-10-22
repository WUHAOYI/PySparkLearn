# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：取出rdd的前N个元素

if __name__ == '__main__':
    conf = SparkConf().setAppName("take").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    result = rdd.take(5)
    print(result)