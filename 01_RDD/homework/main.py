# coding:utf8

from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel
from operator import add

if __name__ == '__main__':
    conf = SparkConf().setAppName("homework").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("hdfs://node1:8020/input/apache.log")

    split_rdd = file_rdd.map(lambda x: x.split(" "))

    split_rdd.persist(StorageLevel.DISK_ONLY)

    print(split_rdd.collect())

    # TODO 需求1:计算当前网站访问的PV(被访问的次数)
    result1 = split_rdd.count()
    print("当前网站访问的PV:", result1)

    # TODO 需求2：当前访问的UV(访问的用户数)
    user_rdd = split_rdd.map(lambda x: x[1]).distinct()
    result2 = user_rdd.count()
    print("访问的用户数：", result2)

    # TODO 需求3：有哪些IP访问了本网站
    ip_rdd = split_rdd.map(lambda x: x[0]).distinct()
    result3 = ip_rdd.collect()
    print("访问了本网站的IP有：", result3)

    # TODO 需求4：那个页面的访问量最高
    page_with_one_rdd = split_rdd.map(lambda x: (x[4], 1))
    result4 = page_with_one_rdd.reduceByKey(add).\
        sortBy(lambda x: x[1],ascending=False,numPartitions=1).\
        first()
    print("访问量最高的网页:",result4)


