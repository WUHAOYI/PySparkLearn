# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能: 将RDD中的数据一条条地按照定义地函数逻辑进行处理

if __name__ == '__main__':
    conf = SparkConf().setAppName("map").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # 1.定义方法来处理数据
    def add(data):
        return data * 10
    print(rdd.map(add).collect())

    # 2.使用lambda表达式
    print(rdd.map(lambda data: data * 10).collect())


