# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将rdd各个分区的数据统一收集到Driver中，形成一个List对象

if __name__ == '__main__':
    conf = SparkConf().setAppName("collect").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15],3)
    print(rdd.collect())