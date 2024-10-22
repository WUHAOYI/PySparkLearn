# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：应用于KV型的rdd，统计各个key出现的次数

if __name__ == '__main__':
    conf = SparkConf().setAppName("countByValue").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 2), ('b', 3)], 3)
    result = rdd.countByValue() # 返回值是一个dict: defaultdict(<class 'int'>, {('a', 1): 2, ('b', 1): 1, ('b', 2): 1, ('b', 3): 1})
    print(result)