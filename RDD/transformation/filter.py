# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD中的数据进行过滤

if __name__ == '__main__':
    conf = SparkConf().setAppName("filter").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
    # 过滤奇数
    odd_rdd = rdd.filter(lambda x: x % 2 == 1)
    # 过滤偶数
    even_rdd = rdd.filter(lambda x: x % 2 == 0)
    print(f'odd-number : {odd_rdd.collect()}')
    print(f'even-number : {even_rdd.collect()}')