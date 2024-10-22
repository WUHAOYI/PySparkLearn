# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：随机抽样rdd的数据

if __name__ == '__main__':
    conf = SparkConf().setAppName("count").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    '''
    params:
    1. 是否重复: True允许重复，False不允许重复
    2. 采样数
    3. 随机种子: 可以不填 如果两次采样填写同一个数字，则两次采样的结果相同
    '''
    rdd = sc.parallelize([1, 3, 5, 3, 1, 3, 2, 6, 7, 8, 6], 1)
    result = rdd.takeSample(False,5,1)
    print(result)