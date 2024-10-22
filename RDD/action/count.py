# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：统计rdd中有多少条数据

if __name__ == '__main__':
    conf = SparkConf().setAppName("count").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    result = rdd.count()
    print(result)