# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD进行自定义分区操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("mapPartitions").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1), ('flink', 1), ('hadoop', 1), ('spark', 1)])

    # 参数: key 可以识别传入的二元组中的key
    def my_partition(key):
        if key == 'hadoop' or key == 'hello':
            return 0
        if key == 'spark':
            return 1
        if key == 'flink':
            return 2

    '''
    params:
    1. numPartitions: 自定义分区个数
    2. func: 自定义分区函数
    '''
    result = rdd.partitionBy(3, my_partition).glom()
    print(result.collect())

    # 需要注意 partitionBy方法只能作用于KV型的RDD
    # 如下所示的例子就会报错: TypeError: cannot unpack non-iterable int object
    # rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9])
    # def my_partition1(key):
    #     if key % 2 == 0:
    #         return 0
    #     else:
    #         return 1
    # result1 = rdd1.partitionBy(2, my_partition1).glom()
    # print(result1.collect())