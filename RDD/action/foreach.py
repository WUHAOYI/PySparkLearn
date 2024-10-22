# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对rdd中的数据按照输入的逻辑进行操作，与map类似，但是没有返回值
# 特点: 不经过driver，由executors直接进行打印输出

if __name__ == '__main__':
    conf = SparkConf().setAppName("foreach").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 1)
    rdd.foreach(lambda x : print(x * 10))