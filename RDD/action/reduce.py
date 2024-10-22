# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：对rdd中的数据按照输入的逻辑进行聚合

if __name__ == '__main__':
    conf = SparkConf().setAppName("reduce").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5])

    # 求和
    total_sum = rdd.reduce(lambda x, y: x + y)

    # 求最大值
    max_value = rdd.reduce(lambda x, y: max(x, y))

    # 求最小值
    min_value = rdd.reduce(lambda x, y: min(x, y))

    print(f'total_sum: {total_sum}')
    print(f'max_value: {max_value}')
    print(f'min_value: {min_value}')