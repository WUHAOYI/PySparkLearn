from pyspark import SparkConf,SparkContext
import os
os.environ['PYSPARK_PYTHON'] = "F:\\anaconda3\\envs\\pyspark\\python.exe"

if __name__ == '__main__':
    print("wordcount")
    conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    #sc.setLogLevel("INFO")
    # 第一步、读取本地数据 封装到RDD集合，认为列表List
    # 不加file默认会读到hdfs上
    wordsRDD = sc.textFile("file:////tmp/pycharm_project_236/data/words.txt")
    print(wordsRDD.collect())
