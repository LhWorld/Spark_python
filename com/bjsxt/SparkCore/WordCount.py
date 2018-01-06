# --encoding:utf-8--
from __future__ import print_function

import sys

from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext(appName="WC")
    #如果Spark读取的是本地文件，那么第一个RDD的分区数=本地文件大小/32M
    #如果Spark读取的是HDFS文件，那么第一个RDD的分区数=本地文件大小/128M
    #第一个RDD的分区数不是与block数严格一致的，是与split数严格一致  split通常情况下与block数一致
    #如果现在有一个文件128.001M  那么在HDFS存储的时候会使用两个block来存，假设第二个block中存储的是最后一条记录的后半部分
    #那么在划分成split的时候，会将第二个block划分到这个split中   split：1  block：2
    lines = sc.textFile("d:/demo/wc")
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda v1,v2:v1+v2)
    #output是一个dict类型          类似java中的map
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
    sc.stop()