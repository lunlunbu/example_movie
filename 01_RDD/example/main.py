# coding:utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import context_jieba,filter_words,append_words,extract_user_and_word
from operator import add

if __name__ == '__main__':
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf=conf)

    # 1.读取文件
    file_rdd = sc.textFile("hdfs://node1:8020/input/SogouQ.txt")

    # 2.切分
    split_rdd = file_rdd.map(lambda x: x.split("\t"))

    # 3.因为有多个需求，所以split_rdd会被多次使用
    split_rdd.persist(StorageLevel.DISK_ONLY)
    print(split_rdd.collect())

    # TODO：需求1：用户搜索的关键词分析
    # 主要分析热点词
    # 取出需要的列
    context_rdd = split_rdd.map(lambda x: x[2])

    # 对搜索内容进行分词分析
    words_rdd = context_rdd.flatMap(context_jieba)

    # print(words_rdd.collect())
    filtered_rdd = words_rdd.filter(filter_words)
    # 将关键字转换
    final_words_rdd = filtered_rdd.map(append_words)
    #对单词进行分布，排序，求出前5名
    result1 = final_words_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1],ascending=False, numPartitions=1).take(5)

    print("需求1结果：",result1)

    # TODO 需求2：用户关键词组合分析
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    #对用户搜索内容进行分词，分词后与用户ID组合、
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_user_and_word)
    result2 = user_word_with_one_rdd.reduceByKey(lambda a, b: a + b).\
        sortBy(lambda x: x[1], ascending=False,numPartitions=1).\
        take(5)

    print("需求2结果：", result2)

    # TODO 需求三:热门搜索时间段分析
    # 取出来所有的时间
    time_rdd = split_rdd.map(lambda x: x[0])
    #对时间进行处理
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))

    result3 = hour_with_one_rdd.reduceByKey(add).\
        sortBy(lambda x: x[1], ascending=False,numPartitions=1).\
        collect()
    print(result3)