# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD中的数据进行重新分区

if __name__ == '__main__':
    conf = SparkConf().setAppName("repartition").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    # 减少分区
    print(rdd.repartition(1).getNumPartitions())

    # 增加分区
    print(rdd.repartition(5).getNumPartitions())