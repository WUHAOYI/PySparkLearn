# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create_by_small_files").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("/spark-data/data/tiny_files")
    file_contents = rdd.map(lambda x: x[1]).collect()
    for content in file_contents:
        print(content)