# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：以分区为单位进行map操作
'''
需要注意map和mapPartitions的func的区别
map: 对 RDD 中的每个元素单独执行传递的函数
mapPartitions: 对每个分区中的整个迭代器执行传递的函数
'''

if __name__ == '__main__':
    conf = SparkConf().setAppName("mapPartitions").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)
    def process(iter):
        result = []
        for it in iter:
            result.append(it * 10)

        return result

    result = rdd.mapPartitions(process)

    # 写成lambda表达式的形式
    result = rdd.mapPartitions(lambda iter : [it * 10 for it in iter])

    print(result.collect())