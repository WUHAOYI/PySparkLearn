# coding:utf8

from pyspark import SparkConf, SparkContext

# 功能：将两个rdd合并为1个rdd

if __name__ == '__main__':
    conf = SparkConf().setAppName("join").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd_employee = sc.parallelize([(1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu")])
    rdd_dept = sc.parallelize([(1001, "销售部"), (1002, "科技部"),(1006,"财务部")])
    # join操作只能作用于二元组
    rdd_join = rdd_employee.join(rdd_dept) # inner join
    rdd_left_join = rdd_employee.leftOuterJoin(rdd_dept) # 左外连接
    rdd_right_join = rdd_employee.rightOuterJoin(rdd_dept) # 右外连接

    print(rdd_join.collect())
    print(rdd_left_join.collect())
    print(rdd_right_join.collect())


