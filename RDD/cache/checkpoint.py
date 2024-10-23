# coding:utf8

from pyspark import SparkConf, SparkContext, StorageLevel

# 功能：对RDD进行缓存

if __name__ == '__main__':
    conf = SparkConf().setAppName("checkpoint").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # checkpoint是讲rdd缓存到磁盘上 可以选择HDFS作为存储路径 与cache相比更加安全
    path = "hdfs://hadoop102:8020/spark-checkpoint"
    sc.setCheckpointDir(path) # 设置存储路径

    # 一个简单的wordcount示例
    rdd = sc.textFile("hdfs://hadoop102:8020/spark-data/data/words.txt")
    rdd1 = rdd.flatMap(lambda line: line.split(' '))
    rdd2 = rdd1.map(lambda word: (word, 1))
    rdd3 = rdd2.reduceByKey(lambda x, y: x + y)
    print(rdd3.collect())

    rdd1.checkpoint() #进行缓存
    rdd4 = rdd1.map(lambda x: x + "-test")
    print(rdd4.collect())
    rdd1.unpersist()  # 缓存不用的时候可以通过unpersist进行清除

