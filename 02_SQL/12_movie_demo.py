# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    # 0.构建执行环境入口SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 1.读取数据集
    schema = StructType().add("user_id", StringType(),nullable=True).\
        add("movie_id", IntegerType(),nullable=True).\
        add("rank", IntegerType(),nullable=True).\
        add("ts", StringType(),nullable=True)

    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("../data/input/u.data")

    df.persist(StorageLevel.DISK_ONLY)

    # TODO 1用户打分平均分
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show()

    # TODO 2 电影平均分查询
    # df.groupBy("movie_id").\
    #     avg("rank"). \
    #     withColumnRenamed("avg(rank)", "avg_rank"). \
    #     withColumn("avg_rank", F.round("avg_rank", 2)). \
    #     orderBy("avg_rank", ascending=False). \
    #     show()

    df.createTempView("movie")
    spark.sql("""
        select movie_id, ROUND(AVG(rank), 2) as avg_rank from movie group by movie_id order by avg_rank desc 
    """).show()

    # TODO 3查询大于平均分的电影数量
    print("大于平均分的电影数量", df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # TODO 4查询高分电影种(大于三分)打分次数最多的用户，此人打分的平均分
    # 找出打分次数最多的人
    user_id = df.where("rank > 3").\
        groupBy("user_id").\
        count().\
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        limit(1).\
        first()['user_id']
    # 计算这个人的打分平均值
    df.filter(df['user_id'] == user_id).\
        select(F.round(F.avg("rank"), 2))

    #TODO 5 查询每个用户平均打分，最低打分，最高打分
    df.groupBy(df["user_id"]).\
        agg(
            F.round(F.avg("rank"), 2).alias("avg_rank"),
            F.min("rank").alias("min_rank"),
            F.max("rank").alias("max_rank")
        ).show()

    # TODO 6 查询评分超过一百次的电影，平均分，排名top10
    df.groupBy("movie_id").\
        agg(
            F.count("movie_id").alias("cnt"),
            F.round(F.avg("rank"), 2).alias("avg_rank")
        ).where("cnt > 100").\
        orderBy("avg_rank", ascending=False).\
        limit(10).\
        show()


