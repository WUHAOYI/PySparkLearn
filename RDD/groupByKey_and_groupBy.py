# coding:utf8
from pyspark import SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("mini").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1)])
    groupBy_rdd = rdd.groupBy(lambda x: x[0])
    print(groupBy_rdd.map(lambda x: (x[0], list(x[1]))).collect())

    groupByKey_rdd = rdd.groupByKey()
    print(groupByKey_rdd.map(lambda x: (x[0], list(x[1]))).collect())



