# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD中的数据进行过滤

if __name__ == '__main__':
    conf = SparkConf().setAppName("distinct").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 1, 2, 2, 2, 3, 3, 3])
    print(rdd.distinct().collect())

