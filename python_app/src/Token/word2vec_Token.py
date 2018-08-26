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


def TXT_Tokenizing_to_text(JSONs_to_TXT_output_path, Token_output_path,stopword_set, En_set, space):
    i = 0
    '''
    删词逻辑
    '''
    space_extractor = re.compile("\s")
    num_start_extractor = re.compile("^[第一两二三四五六七八九十白千万兆亿几]+.{0,}$")
    English_word_extractor = re.compile("^[a-zA-Z0-9]+$")
    Single_word_extractor = re.compile("^.$")

    dict = {}
    with open(Token_output_path, encoding="utf-8", mode="a") as data:
        with open(JSONs_to_TXT_output_path, 'r', encoding='utf-8') as f:
            for dataline in f:
                if i % 10000 == 0:
                    logging.log(logging.CRITICAL, " Tokenizing : %d -> %d" % (i, i + 10000))
                i = i + 1
                generator = jieba.cut(dataline, HMM=False)

                result = ""

                for word in generator:
                    '''
                    小写字母处理
                    '''
                    word = word.lower()

                    se_count = len(space_extractor.findall(word))
                    nse_count = len(num_start_extractor.findall(word))
                    Ewe_count = len(English_word_extractor.findall(word))
                    sw_count = len(Single_word_extractor.findall(word))

                    if word not in stopword_set and se_count == 0 and nse_count == 0 and Ewe_count == 0 and sw_count == 0:
                        result = result + word + space
                        if dict.get(word) != None:
                            dict[word] = dict[word] + 1
                        else:
                            dict[word] = 1
                    elif word in En_set:
                        result = result + word + space
                        if dict.get(word) != None:
                            dict[word] = dict[word] + 1
                        else:
                            dict[word] = 1

                result = result[:-1] + '\n'
                data.write(result)
    return dict


def loading_reusing_word_from_userdict(jieba_userdict_path):
    English_word_extractor = re.compile("^[a-zA-Z0-9]+ ")
    Single_word_extractor = re.compile("^. ")
    English_word_set = set()
    with open(jieba_userdict_path, 'r', encoding='utf-8') as f:
        for dataline in f:
            if len(English_word_extractor.findall(dataline)) != 0:
                English_word = English_word_extractor.findall(dataline)[0][:-1]
                English_word_set.add(English_word)
            if len(Single_word_extractor.findall(dataline)) !=0:
                Single_word =  Single_word_extractor.findall(dataline)[0][:-1]
                English_word_set.add(Single_word)

        return English_word_set


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
            sql = "insert into word2index_0810 (word,frequency) values ('%s',%d)" % (word, dict[word])
            cur.execute(sql)
        except:
            try:
                sql = 'insert into word2index_0810 (word,frequency) values ("%s",%d)' % (word, dict[word])
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
    Token：读取stopword_set
    '''
    jieba_stopword_set_path = "/Users/choukichiou/Desktop/dicts/chinese_stopword.txt"
    stopword_set = loading_stop_word_set(jieba_stopword_set_path)

    '''
    Token：添加自定义词典
    '''
    jieba_userdict_path = "/Users/choukichiou/Desktop/dicts/jieba_dict.txt"
    loading_jieba_userdict(jieba_userdict_path)

    '''
    从dict获取复用的词库
    '''
    En_set = loading_reusing_word_from_userdict(jieba_userdict_path)

    '''
    JSONs_to_TXT_output_path：初始语料地址，已去除百科结构
    Token_output_path：分词后存储地址
    stopword_set：停用词表
    En_set：从分词库中提取的复用词表
    space：分隔符
    '''
    Token_output_path = "/Users/choukichiou/Desktop/0810RESULT/detail_0810.txt"
    word_dict = TXT_Tokenizing_to_text(JSONs_to_TXT_output_path, Token_output_path,stopword_set, En_set, "_")

    '''
    word_dict入库
    '''
    Storing_word_dict_toDB(word_dict)


'''
word2vec分词：

1、 构建分词所用词库
2、 确定需要筛除的词的逻辑，添加到TXT_Tokenizing_to_text之中
3、 确定需要复用的词（符合筛除逻辑，但是存在与词库之中的词）


'''