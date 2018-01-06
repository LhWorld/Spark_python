#--encoding:utf-8--
'''
Created on 2017年10月20日

@author: root
'''
from pyspark import SparkConf,SparkContext
from __builtin__ import str
conf = SparkConf().setMaster("local").setAppName("AggregateByKey")
sc = SparkContext(conf = conf)

rdd = sc.parallelize([(1,1),(1,2),(2,1),(2,3),(2,4),(1,7)],2)

def f(index,items):
    print "partitionId:%d" %index
    for val in items:
        print val
    return items
    
rdd.mapPartitionsWithIndex(f, False).count()


def seqFunc(a,b):
    print "seqFunc:%s,%s" %(a,b)
    return max(a,b)
def combFunc(a,b):
    print "combFunc:%s,%s" %(a ,b)
    return a + b
'''
    aggregateByKey这个算子内部肯定有分组
'''
aggregateRDD = rdd.aggregateByKey(3, seqFunc, combFunc)
rest = aggregateRDD.collectAsMap()
for k,v in rest.items():
    print k,v

sc.stop()





