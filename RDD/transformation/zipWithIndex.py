# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将 RDD 中的每个元素与其对应的索引（从 0 开始）配对成一个元组
# 注意：全局进行索引

if __name__ == '__main__':
    conf = SparkConf().setAppName("zipWithIndex").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(['a', 'b', 'c', 'd'], 2)
    result = rdd.zipWithIndex()
    print(result.collect())


