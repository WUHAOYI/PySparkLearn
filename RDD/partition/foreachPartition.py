# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：和foreach类似，一次处理一整个分区的数据
# 注意：与foreach的区别也是在于传递的函数所作用的对象不同 (参考map和mapPartitions)

if __name__ == '__main__':
    conf = SparkConf().setAppName("foreachPartition").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)
    # foreachPartition 没有固定的输出顺序，哪一个分区先处理完就先输出
    rdd.foreachPartition(lambda iter : print([it * 10 for it in iter]))
