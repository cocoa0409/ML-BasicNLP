import json
import logging
import re
import pymysql
import jieba


def linkandh2h3formatchange_advanced(text):
    link = re.compile(r'<a.+?href.*?/item/.*?</a>')
    text = link.sub(replace_link_advanced, text)
    h2h3 = re.compile(r'<h.+?</h.>')
    text = h2h3.sub('', text)
    enter = re.compile(r'\n')
    text = enter.sub('，', text)
    return text


def replace_link_advanced(input):
    value = input.group()
    text = re.findall(r'>[^\s\n]+?<', value)
    if len(text) == 0:
        text = re.findall(r'>[^\n]+?<', value)
    if len(text) == 0:
        return '\n'
    text = text[0]
    text = text[1:len(text) - 1]
    # 解决加粗文本问题
    if '<b>' in text or '<i>' in text:
        text = text[3:len(text)]
    return text


def JSONs_to_TXT(detail_text_set, JSONs_to_TXT_output_path):
    i = 0
    topic_rmd_set = set()
    with open(JSONs_to_TXT_output_path, encoding="utf-8", mode="a") as data:
        for text in detail_text_set:
            with open(text, 'r', encoding='utf-8') as f:
                for dataline in f:
                    if i % 10000 == 0:
                        logging.log(logging.CRITICAL, " JSONs to TXT : %d -> %d" % (i, i + 10000))
                    i = i + 1

                    JSON = json.loads(dataline)
                    if JSON['topic'] in topic_rmd_set:
                        continue
                    else:
                        topic_rmd_set.add(JSON['topic'])
                    data.write(linkandh2h3formatchange_advanced(JSON['detail_text']) + "\n")


def TXT_Tokenizing_to_text(Spark_dict_path,Spark_limit,JSONs_to_TXT_output_path,Spark_output_path,space):

    '''
    加载完整词典
    '''
    dict_set=loading_stop_word_set(Spark_dict_path)

    i = 0

    dict = {}

    total_num=0
    spark_total_num=0

    with open(Spark_output_path, encoding="utf-8", mode="a") as spark:
        with open(JSONs_to_TXT_output_path, 'r', encoding='utf-8') as f:
            for dataline in f:
                if i % 10000 == 0:
                    logging.log(logging.CRITICAL, " Tokenizing : %d -> %d" % (i, i + 10000))
                i = i + 1
                generator = jieba.cut(dataline, HMM=False)

                result = ""
                '''
                记录处于我们词典的个数
                '''
                count_for_spark_significant_result = 0

                for word in generator:
                    '''
                    记录处于我们词典的个数
                    '''
                    if word in dict_set:
                        count_for_spark_significant_result=count_for_spark_significant_result+1
                        result = result + word + space
                        if dict.get(word) != None:
                            dict[word] = dict[word] + 1
                        else:
                            dict[word] = 1

                result = result[:-1] + '\n'
                total_num=total_num+1
                if count_for_spark_significant_result> Spark_limit:
                    spark.write(result)
                    spark_total_num=spark_total_num+1

    return dict,total_num,spark_total_num


def loading_jieba_userdict(path):
    logging.log(logging.CRITICAL, " [Constructing] user dict")
    jieba.load_userdict(path)
    logging.log(logging.CRITICAL, " [Constructing] user dict finish")


def loading_stop_word_set(path):
    stopword_set = set()
    logging.log(logging.CRITICAL, " [Constructing] stopword dict")
    with open(path, encoding="utf-8", mode="r") as stp:
        for dataline in stp:
            stopword_set.add(dataline[:-1])
    logging.log(logging.CRITICAL, " [Constructing] stopword dict finish")
    return stopword_set


def Storing_word_dict_toDB(dict):
    host = '127.0.0.1'
    user = 'root'
    # 你自己数据库的密码
    psd = 'Mama1203.'
    port = 3306
    # 你自己数据库的名称
    db = 'LDA'
    charset = 'utf8'
    # 数据库连接
    con = pymysql.connect(host=host, user=user, passwd=psd, db=db, charset=charset, port=port)
    # 数据库游标
    cur = con.cursor()

    logging.log(logging.CRITICAL, "[logging] Storing Start ")
    logging.log(logging.CRITICAL, "[logging] total item : " + str(len(dict)))
    i = 0
    for word in dict:
        if i % 10000 == 0:
            logging.log(logging.CRITICAL, " word2index Storing: %d -> %d" % (i, i + 10000))
            con.commit()
        i = i + 1
        try:
            sql = "insert into word2index_0814 (word,frequency) values ('%s',%d)" % (word, dict[word])
            cur.execute(sql)
        except:
            try:
                sql = 'insert into word2index_0814 (word,frequency) values ("%s",%d)' % (word, dict[word])
                cur.execute(sql)
            except:
                logging.log(logging.CRITICAL, " UNKNWON TYPE FOUND in INSERT : %s" % word)
    logging.log(logging.CRITICAL, "[logging] Storing Finish ")
    con.commit()
    con.close()

if __name__ == '__main__':
    '''
    指定输出目录
    指定输入目录
    '''
    detail_text_set = []
    detail_text_set.append("/Users/choukichiou/Desktop/BaikeCrawler/output_files/JSON_P6yfu30M.json")
    detail_text_set.append("/Users/choukichiou/Desktop/BaikeCrawler/output_files/JSON_qkXMQlvE.json")
    detail_text_set.append("/Users/choukichiou/Desktop/BaikeCrawler/output_files/JSON_S6C8NiE1.json")

    '''
    先对输入的爬虫数据做处理，清理成可阅读的字符串形式，方便以后直接从下一步开始分词，不用再百科转换
    '''
    JSONs_to_TXT_output_path = "/Users/choukichiou/Desktop/0808RESULT/corpus_from_baike_0808.txt"
    # JSONs_to_TXT(detail_text_set, JSONs_to_TXT_output_path)

    '''
    Token：添加自定义词典
    '''
    jieba_userdict_path = "/Users/choukichiou/Desktop/dicts_for_LDA/jieba_dict.txt"
    loading_jieba_userdict(jieba_userdict_path)

    '''
    Token：分词并以下划线分割，并以dict返回统计信息
    '''
    Spark_dict_path = "/Users/choukichiou/Desktop/dicts_for_LDA/Spark_dict.txt"
    Spark_output_path = "/Users/choukichiou/Desktop/0814RESULT/Spark_detail_0814.txt"
    word_dict,total_num,spark_total_num = TXT_Tokenizing_to_text(Spark_dict_path,100,JSONs_to_TXT_output_path,Spark_output_path,space="_")
    '''
    word_dict入库word2index_0814
    '''
    Storing_word_dict_toDB(word_dict)

    '''
    输出Spark信息
    '''
    for i in range(0,10):
        logging.log(logging.CRITICAL," [INFO] Total_Num = "+str(total_num)+", Spark_Total_Num = "+str(spark_total_num))



'''
LDA分词：

1、 构建分词所用词库，已经事先去除了stopword中的词
2、 输出中的分词后文本仅包含词库中的词

'''
