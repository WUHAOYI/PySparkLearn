# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：针对KV型rdd，根据key进行排序

if __name__ == '__main__':
    conf = SparkConf().setAppName("sortByKey").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    '''
    params:
    1.ascending: True为升序 False为降序
    2.numPartitions: 分为几个分区来进行排序
    3.keyFunc: 在排序前对key进行的预处理操作
    '''
    rdd = sc.parallelize([('a', 1), ('E', 1), ('C', 1), ('D', 1), ('b', 1), ('g', 1), ('f', 1),
                          ('y', 1), ('u', 1), ('i', 1), ('o', 1), ('p', 1),
                          ('m', 1), ('n', 1), ('j', 1), ('k', 1), ('l', 1)], 3)

    # 将key转化为小写，然后排序
    # 注意 这里对key的转化只应用于排序过程中，并不会改变rdd中的实际数据
    result = rdd.sortByKey(ascending=True, numPartitions=1,keyfunc=lambda x:str(x).lower())
    print(result.collect())