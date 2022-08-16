import json
import csv
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructField, StructType

# 初始化spark对象
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

schemaString = "budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count"
fields = [StructField(field, StringType(), True)
          for field in schemaString.split(",")]
schema = StructType(fields)

moviesRdd = sc.textFile('movies.csv').map(
    lambda line: Row(*next(csv.reader([line]))))
mdf = spark.createDataFrame(moviesRdd, schema)


# 保存成json文件
def countByJson(field):
    return mdf.select(field).filter(mdf[field] != '').rdd.flatMap(
        lambda g: [(v, 1) for v in map(lambda x: x['name'], json.loads(g[field]))]).repartition(1).reduceByKey(
        lambda x, y: x + y)


# 体裁统计
def countByGenres():
    res = countByJson("genres").collect()
    return list(map(lambda v: {"genre": v[0], "count": v[1]}, res))


# 关键词词云
def countByKeywords():
    res = countByJson("keywords").sortBy(lambda x: -x[1]).take(100)
    return list(map(lambda v: {"x": v[0], "value": v[1]}, res))


# 语言统计
def countByLanguage():
    res = countByJson("spoken_languages").filter(
        lambda v: v[0] != '').sortBy(lambda x: -x[1]).take(10)
    return list(map(lambda v: {"language": v[0], "count": v[1]}, res))


# 电影时长分布
def distrbutionOfRuntime(order='count', ascending=False):
    return mdf.filter(mdf["runtime"] != 0).groupBy("runtime").count().filter('count>=0').toJSON().map(
        lambda j: json.loads(j)).collect()


# 上映时间评价关系
def dateVote():
    return mdf.select(mdf["release_date"], "vote_average", "title").filter(mdf["release_date"] != "").filter(
        mdf["vote_count"] > 100).collect()


# 流行度评价关系
def popVote():
    return mdf.select("title", "popularity", "vote_average").filter(mdf["popularity"] != 0).filter(
        mdf["vote_count"] > 100).collect()


def save(path, data):
    with open(path, 'w') as f:
        f.write(data)


if __name__ == "__main__":
    m = {
        "countByGenres": {
            "method": countByGenres,
            "path": "genres.json"
        },
        "countByKeywords": {
            "method": countByKeywords,
            "path": "keywords.json"
        },

        "countByLanguage": {
            "method": countByLanguage,
            "path": "language.json"
        },
        "distrbutionOfRuntime": {
            "method": distrbutionOfRuntime,
            "path": "runtime.json"
        },
        "dateVote": {
            "method": dateVote,
            "path": "date_vote.json"
        },
        "popVote": {
            "method": popVote,
            "path": "pop_vote.json"
        }

    }
    base = "static/"
    if not os.path.exists(base):
        os.mkdir(base)

    for k in m:
        p = m[k]
        f = p["method"]
        save(base + m[k]["path"], json.dumps(f()))
        print("done -> " + k + " , save to -> " + base + m[k]["path"])

    print('电影数据分析结果已保存在文件static中。')
    # save("test.jj", json.dumps(countByGenres()))
