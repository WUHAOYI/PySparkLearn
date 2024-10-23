# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：获取数据的分区信息

if __name__ == '__main__':
    conf = SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    def index_data(partition_index, iterator): # 方法必须有返回值
        # 为每个元素加上分区索引信息
        return [(partition_index, item) for item in iterator]


    # 使用 mapPartitionsWithIndex 来标注每条数据的分区
    result = rdd.mapPartitionsWithIndex(index_data)

    # 查看每条数据及其所属的分区
    print(result.collect())

