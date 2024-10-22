# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对rdd中的数据进行降序排序，取出前N个组成list返回

if __name__ == '__main__':
    conf = SparkConf().setAppName("top").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    result = rdd.top(3)
    print(result)