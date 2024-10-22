# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对RDD的对象执行map操作，并解除嵌套

if __name__ == '__main__':
    conf = SparkConf().setAppName("flatMap").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # example1: 传入一个list来创建rdd list的内容是一个字符串
    rdd = sc.parallelize(["hadoop spark hadoop", "spark hadoop hadoop", "hadoop flink spark"])
    # 对list进行解嵌套
    rdd2 = rdd.flatMap(lambda line: line.split(" "))
    print(rdd2.collect())

    # example2: 传入一个list来创建rdd list的内容是一个字符串
    rdd3 = sc.parallelize([[1,2,3],[4,5,6],[7,8,9]])
    rdd4 = rdd3.flatMap(lambda line: (line[i] for i in range(len(line))))
    print(rdd4.collect())