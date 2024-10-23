# coding:utf8

from pyspark import SparkConf, SparkContext, StorageLevel

# 功能：对RDD进行缓存

if __name__ == '__main__':
    conf = SparkConf().setAppName("cache").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 一个简单的wordcount示例
    rdd = sc.textFile("hdfs://hadoop102:8020/spark-data/data/words.txt")
    rdd1 = rdd.flatMap(lambda line: line.split(' '))
    rdd2 = rdd1.map(lambda word: (word, 1))
    rdd3 = rdd2.reduceByKey(lambda x, y: x + y)
    print(rdd3.collect())

    # 在上面的示例中 构建完rdd2之后rdd1就不存在了
    # 下面第二次使用rdd1的时候 需要根据rdd的血缘关系重新从头开始构建出rdd1来，供rdd4使用
    # rdd4 = rdd1.map(lambda x: x + "-test")
    # print(rdd4.collect())

    # 通过缓存技术 可以避免rdd的重复构建
    rdd1.cache()
    rdd4 = rdd1.map(lambda x: x + "-test")
    print(rdd4.collect())
    rdd1.unpersist() # 缓存不用的时候可以通过unpersist进行清除

    # 使用cache是缓存在内存中 也可以设置缓存在磁盘上
    # rdd1.persist(StorageLevel.MEMORY_ONLY) # 仅内存缓存 cache的实现就是MEMORY_ONLY
    # rdd1.persist(StorageLevel.DISK_ONLY) # 仅磁盘缓存
    # rdd1.persist(StorageLevel.MEMORY_AND_DISK) # 先内存缓存，空间不够再去磁盘缓存
    # 更多缓存方式可以查看persist的源码

