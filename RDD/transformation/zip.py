# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将两个RDD中相同位置的元素配对成一个KV型RDD
# 注意：要求两个 RDD 的分区数和每个分区内的元素数都相同，否则会抛出异常: Can only zip RDDs with same number of elements in each partition

if __name__ == '__main__':
    conf = SparkConf().setAppName("zip").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3], 3)
    rdd2 = sc.parallelize(['a', 'b', 'c'], 3)
    result = rdd1.zip(rdd2)
    print(result.collect())

