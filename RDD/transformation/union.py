# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将两个rdd合并为1个rdd

if __name__ == '__main__':
    conf = SparkConf().setAppName("union").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 1, 3, 3])
    rdd2 = sc.parallelize(["a", "b", "a", (1, 23), [1, 2]])
    rdd3 = rdd1.union(rdd2)
    print(rdd3.collect())