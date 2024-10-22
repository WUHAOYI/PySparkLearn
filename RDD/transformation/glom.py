# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对rdd的数据按照分区来进行嵌套

if __name__ == '__main__':
    conf = SparkConf().setAppName("glom").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 2)
    result1 = rdd1.glom()
    print(result1.collect())

    rdd2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    result2 = rdd2.glom()
    print(result2.collect())

    # 嵌套是以分区来进行的!