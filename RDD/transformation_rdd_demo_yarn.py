# coding:utf8

import json
from pyspark import SparkConf,SparkContext
import os
# 告知yarn路径
os.environ["HADOOP_CONF_DIR"] = "/opt/module/hadoop/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("yarn_test_pycharm").setMaster("yarn")
    sc = SparkContext(conf=conf)

    # rdd = sc.textFile("../data/order.text")
    # 修改文件路径为hdfs
    rdd = sc.textFile("hdfs://hadoop102:8020/testData/order.text")

    print(rdd.collect())

    json_str_rdd = rdd.flatMap(lambda line: line.split("|"))

    json_dic_rdd = json_str_rdd.map(lambda x: json.loads(x))

    target_rdd = json_dic_rdd.filter(lambda x: x["areaName"] == "北京")

    result = target_rdd.map(lambda x: x["areaName"] + "_" + x["category"])

    print(result.distinct().collect())

