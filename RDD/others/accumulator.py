# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：从各个executor对象中收集运行结果并作用于其自身

if __name__ == '__main__':
    conf = SparkConf().setAppName("accumulator").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    # 1. 普通计数方法：输出结果为0
    # 因为最初定义的count来自于driver，而当map需要count对象时，driver会将count对象复制一份，发送给各个executor
    # 所以executor中的count和driver中的count对象是无关的
    count = 0


    def map_func(index, iter):
        global count
        result = []
        for item in iter:
            count += 1
            print(f'partition {index} : {count}')
            result.append((index,item))
        return result

    result = rdd.mapPartitionsWithIndex(map_func).collect()
    print(result)
    print(count)

    # 2. 使用累加器
    acmlt = sc.accumulator(0)


    def map_func2(data):
        global acmlt
        acmlt += 1


    rdd.map(map_func2).collect()
    print(acmlt)
