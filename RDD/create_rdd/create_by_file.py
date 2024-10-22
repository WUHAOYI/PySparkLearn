# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create_by_file").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1.读取windows本地文件
    # file_rdd_windows = sc.textFile("../data/input/words.txt")
    # print("默认分区数量为:", file_rdd_windows.getNumPartitions())
    # print("content:", file_rdd_windows.collect())

    # 2.读取linux本地文件
    '''
    注意: 
    1.Linux本地文件系统路径前要加上:file://，否则会默认去读取hdfs上的文件
    2.读取Linux本地文件时需要使用绝对路径
    '''
    file_rdd_linux = sc.textFile("file:///tmp/pycharm_project_7/data/words.txt")
    print("默认分区数量为:", file_rdd_linux.getNumPartitions())
    print("content:", file_rdd_linux.collect())

    # 手动设置分区数量 但该分区数量spark会根据自己的判断来设置，设置太大的值不会生效
    file_rdd_linux = sc.textFile("file:///tmp/pycharm_project_7/data/words.txt",100)
    print("设置的分区数量为:", file_rdd_linux.getNumPartitions())
    print("content:", file_rdd_linux.collect())

    # 3.读取HDFS文件
    file_rdd_HDFS = sc.textFile("/spark-data/data/words.txt")
    print("默认分区数量为:", file_rdd_HDFS.getNumPartitions())
    print("content:", file_rdd_HDFS.collect())