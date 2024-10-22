# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD进行排序，取前N个

if __name__ == '__main__':
    conf = SparkConf().setAppName("takeOrdered").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    '''
    params:
    1. 取数数量
    2. func: 排序逻辑（默认是升序）
    '''
    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 1)
    # 升序
    result1 = rdd.takeOrdered(5)
    # 降序
    result2 = rdd.takeOrdered(5,lambda x : -x)

    print(result1)
    print(result2)
