# coding:utf8

from pyspark import SparkConf,SparkContext

# 并行化创建RDD: parallelize
if __name__ == '__main__':
    conf = SparkConf().setAppName("create_by_local_data").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 默认分区 根据CPU cores来确定
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print("默认分区数:", rdd.getNumPartitions())
    print("content:", rdd.collect())

    # 设置分区数
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    print("分区数:", rdd.getNumPartitions())
    print("content:", rdd.collect())
