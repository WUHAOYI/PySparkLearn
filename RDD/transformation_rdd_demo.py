# coding:utf8
import json
from pyspark import SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("mini").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/order.text")

    json_str_rdd = rdd.flatMap(lambda line: line.split("|"))

    json_dic_rdd = json_str_rdd.map(lambda x: json.loads(x))

    target_rdd = json_dic_rdd.filter(lambda x: x["areaName"] == "北京")

    result = target_rdd.map(lambda x: x["areaName"] + "_" + x["category"])

    print(result.distinct().collect())

