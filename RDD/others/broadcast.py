# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将本地数据分发到各个Executor中，每个Executor内只保留一份数据，Executor内部的各个线程（分区）就可以共享这一份数据

if __name__ == '__main__':
    conf = SparkConf().setAppName("checkpoint").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    local_list = [1,2,3] # 本地变量
    broadcast_list = sc.broadcast(local_list) # 进行广播

    rdd = sc.parallelize(['a','b','c'])
    value = broadcast_list.value # 从广播变量中取值
    print(value)

    result = rdd.zip(sc.parallelize(value))
    print(result.collect())
