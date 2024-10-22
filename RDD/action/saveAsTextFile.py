# coding:utf8
import time

from pyspark import SparkConf, SparkContext

# 功能：应用于KV型的rdd，统计各个key出现的次数

if __name__ == '__main__':
    conf = SparkConf().setAppName("saveAsTextFile").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)
    # 保存到HDFS
    # 1. 保存的文件及其校验文件都保存在一个文件夹内，而这个文件夹不能提前创建好
    # 2. 保存文件是分布式执行的，不经过driver，所以每一个分区都会产生一个保存文件

    # 保存到本地
    file_path = "file:///tmp/pycharm_project_7/data/output/out-" + str(time.time())
    rdd.saveAsTextFile(file_path)
