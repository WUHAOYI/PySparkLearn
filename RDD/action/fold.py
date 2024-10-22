# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对rdd中的数据按照输入的逻辑进行聚合, 但聚合时需要携带初始值

if __name__ == '__main__':
    conf = SparkConf().setAppName("fold").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    '''
    params:
    1.value: 初始值
    2.func: 聚合逻辑
    '''
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    '''
    聚合过程:
    1. 各个分区中分别进行聚合，每个分区聚合时都要加上初始值
    2. 分区之间进行聚合，同样也要加上初始值
    '''
    result = rdd.fold(10, lambda x, y: x + y)
    print(result)
