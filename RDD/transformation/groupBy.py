# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将RDD的数据进行分组

if __name__ == '__main__':
    conf = SparkConf().setAppName("groupBy").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1), ('b', 2), ('b', 3)])
    # 根据key进行分组 t代表的是每一个元组，t[0]代表的就是key
    result = rdd.groupBy(lambda t: t[0])
    print(result.collect())  # [('b', <pyspark.resultiterable.ResultIterable object at 0x7f300acb5400>), ('a', <pyspark.resultiterable.ResultIterable object at 0x7f300acb52e0>)]

    # 以list的形式直观展示
    for key, values in result.collect():
        values = list(values)
        print(f'key:{key}, values:{values}')
