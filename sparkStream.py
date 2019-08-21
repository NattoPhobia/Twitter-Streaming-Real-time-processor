from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def load(sentenceList):
    sentences = {}
    f = open(sentenceList, 'rU')
    txt = f.read()
    txt = txt.split('\n')
    for line in txt:
        sentences[line] = 1
    f.close()
    return sentences


def constructPlot(counts):
    pWordsCounts = []
    nWordsCounts = []

    for feelingFields in counts:
        # val[0] negative field, val[1] positive field
        pWordsCounts.append(feelingFields[0][1])
        nWordsCounts.append(feelingFields[1][1])

    time = []
    for i in range(len(counts)):
        time.append(i)

    posLine = plt.plot(time, pWordsCounts,'ro-', label='pfeelings words')
    negLine = plt.plot(time, nWordsCounts,'ko-', label='nfeelings words')
    plt.axis([0, len(counts), 0, max(max(pWordsCounts), max(nWordsCounts))+40])
    plt.xlabel('time')
    plt.ylabel('count')
    plt.legend(loc = 'upper right')
    plt.savefig('feelingAnalysis.png')


def sumCount(newCount, currentCount):
    return sum(newCount) + (currentCount or 0)


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("twitterStream")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 15)  # batch interval 15 seconds
    ssc.checkpoint("checkpoint")

    # load
    nFeelingWords = load("./Dataset/nFeeling.txt")
    pFeelingWords = load("./Dataset/pFeeling.txt")


    # accept kafka data
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = ['twitterStream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))
    words = tweets.flatMap(lambda line:line.split(" "))
    nfeelings = words.map(lambda word: ('nfeelings', 1) if word in nFeelingWords else ('nfeelings', 0))
    pfeelings = words.map(lambda word: ('pfeelings', 1) if word in pFeelingWords else ('pfeelings', 0))
    bothFeelings = pfeelings.union(nfeelings)
    feelingCounts = bothFeelings.reduceByKey(lambda x,y: x+y)
    currentFeelingCounts = feelingCounts.updateStateByKey(sumCount)
    currentFeelingCounts.pprint()

    counts = []
    feelingCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    ssc.start()
    ssc.awaitTerminationOrTimeout(45)
    ssc.stop(stopGraceFully = True)
    constructPlot(counts)

if __name__=="__main__":
    main()

