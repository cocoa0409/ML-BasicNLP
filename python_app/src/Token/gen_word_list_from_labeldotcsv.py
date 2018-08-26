import re
import logging


def loading_stop_word_set(path):
    stopword_set = set()
    logging.log(logging.CRITICAL, " [Constructing] stopword dict")
    with open(path, encoding="utf-8", mode="r") as stp:
        for dataline in stp:
            stopword_set.add(dataline[:-1])
    logging.log(logging.CRITICAL, " [Constructing] stopword dict finish")
    return stopword_set

stop_word_path="/Users/choukichiou/Desktop/dicts/chinese_stopword.txt"
stop_word_set = loading_stop_word_set(stop_word_path)



i = 0
candidate_topic_extractor = re.compile("^'[^0-9一二三四五六七八九十,，\s!！][^0-9一二三四五六七八九十,，\s!！]+?,\d{2,}")
topic_extractor = re.compile("^'.+?,")
num_extractor = re.compile(",\d+?$")
rm_parenthesis = re.compile("（.+）")

enword_extractor = re.compile("[a-z]")

label_csv_path = "/Users/choukichiou/Documents/Gitlab/wiki-miner/extract/final-zh/label.csv"
topic_output_path="/Users/choukichiou/Desktop/dicts/jieba_dict.txt"
spark_output_path="/Users/choukichiou/Desktop/dicts/Spark_dict.txt"

rmdup = {}
threshold = 20
with open(label_csv_path,'r',encoding='utf-8') as f:
    for dataline in f:
        if i % 10000==0:
            logging.log(logging.CRITICAL," 从label.csv提取词表至内存 : %d,%d"%(i,i+10000))
        i=i+1
        try:
            candidate_topic=candidate_topic_extractor.findall(dataline)[0]
        except:
            continue
        topic = topic_extractor.findall(candidate_topic)[0][1:-1]
        nums = int(num_extractor.findall(candidate_topic)[0][1:])
        topic=rm_parenthesis.sub('',topic)
        topic=topic.lower()

        enword_count =enword_extractor.findall(topic)

        if nums>= threshold and topic not in stop_word_set and len(enword_count)==0:
            if topic in rmdup.keys():
                rmdup[topic]=rmdup[topic]+int(nums)
            else:
                rmdup[topic]=int(nums)


result=sorted(rmdup.items(),key=lambda item:-item[1])


# single_word_extractor = re.compile("^.$")
# en_extractor = re.compile("^[a-z]$")

i=0
with open(topic_output_path, encoding="utf-8", mode="a") as data:
    for topic in result:
        if i % 10000==0:
            logging.log(logging.CRITICAL," 从内存写入提取后的jieba词表 : %d,%d"%(i,i+10000))
        i=i+1
        data.write(topic[0]+' '+str(topic[1]*10000)+'\n')

i=0
with open(spark_output_path, encoding="utf-8", mode="a") as data:
    for topic in result:
        if i % 10000==0:
            logging.log(logging.CRITICAL," 从内存写入提取后的spark词表 : %d,%d"%(i,i+10000))
        i=i+1
        data.write(topic[0]+'\n')
