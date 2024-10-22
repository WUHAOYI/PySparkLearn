# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对rdd的数据进行排序

if __name__ == '__main__':
    conf = SparkConf().setAppName("sortBy").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    '''
    params:
    1.func: 排序规则
    2.ascending: True为升序 False为降序
    3.numPartitions: 分为几个分区来进行排序
    需要注意的是，sortBy的结果永远是全局排序的，也就是说，无论numPartitions设置为多少，排序后的数据顺序是唯一的
    '''

    rdd = sc.parallelize([('c', 3), ('f', 1), ('b', 11), ('c', 3), ('a', 1), ('c', 5), ('e', 1), ('n', 9), ('a', 1)], 3)
    result = rdd.sortBy(lambda x: x[0], ascending=True, numPartitions=1)
    print(result.collect())

    result_multi_partitions = rdd.sortBy(lambda x: x[0],ascending=True,numPartitions=3)
    print(result_multi_partitions.collect())

    # 通过glom查看数据在分区内的排序情况
    result_multi_partitions_glom = rdd.sortBy(lambda x: x[0],ascending=True,numPartitions=3).glom()
    print(result_multi_partitions_glom.collect())