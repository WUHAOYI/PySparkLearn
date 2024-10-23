# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD中的数据进行重新分区
# 与repartition的区别在于可以设置是否允许增加分区
# 使用coalesce时，shuffle为False时，减少分区不会触发shuffle; 而使用repartition时，无论增加还是减少分区，都会触发shuffle
# shuffle参数的含义是是否允许触发shuffle，并非是否允许增加分区；只是说shuffle为False时不允许增加分区，但shuffle为True时既可以增加分区，也可以减少分区

if __name__ == '__main__':
    conf = SparkConf().setAppName("coalesce").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

    '''
    params:
    1. N: number of partitions
    2. shuffle: 是否触发shuffle, 默认为False 
    '''
    # shuffle为False 只能减少分区
    print(rdd.coalesce(1).getNumPartitions())

    # shuffle为True 可以增加分区
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())

    # shuffle为True 也可以减少分区
    print(rdd.coalesce(1, shuffle=True).getNumPartitions())