from pyspark.mllib.feature import Word2VecModel
import jieba,logging
from pyspark import SparkContext
from pyspark import SparkConf
import numpy as np

def Connect_to_Spark():
    logging.log(logging.CRITICAL," Connecting to Spark")
    conf=SparkConf().\
    setAppName("miniProject").\
    setMaster("local[*]").\
    set('spark.executor.memory','6g').\
    set('spark.driver.maxResultSize', '4g').\
    set('spark.executor.memory','6g').\
    set('spark.driver.memory','10g')
    sc=SparkContext.getOrCreate(conf)
    return sc

def loading_word2vec_Model(sc,path):
    logging.log(logging.CRITICAL," [Loading] Model")
    Model = Word2VecModel.load(sc,path)
    logging.log(logging.CRITICAL," [Loading] Model finish")
    return Model

def loading_jieba_userdict(path):
    logging.log(logging.CRITICAL," [Constructing] user dict")
    jieba.load_userdict(path)
    logging.log(logging.CRITICAL," [Constructing] user dict finish")

def loading_stop_word_set(path):
    stopword_set=set()
    logging.log(logging.CRITICAL," [Constructing] stopword dict")
    with open(path, encoding="utf-8", mode="r") as stp:
        for dataline in stp:
            stopword_set.add(dataline[:-1])
    logging.log(logging.CRITICAL," [Constructing] stopword dict finish")
    return stopword_set

def generate_sentence_vector(doc,Model,stopword_set):
    short_doc1_cut = jieba.cut(doc,HMM=False)
    result = 0
    num = 0
    for word in short_doc1_cut:
        if word in stopword_set:
            continue
        else:
            try:
                word_vector = Model.transform(word)
                result = result + word_vector
                num = num + 1
            except:
                continue
                #print("\033[32mWord \'" + word + "\' is not in vocabulary")
    return result,num

def cosin_similarity(result_first,result_second):
    result_first_norm = result_first / np.sqrt(sum(result_first * result_first))
    result_second_norm = result_second / np.sqrt(sum(result_second * result_second))
    print("\033[34mSimilarity = " + str(sum(result_first_norm  * result_second_norm)))


def cal_short_doc_distance(Model,stopword_set):
    while(True):
        '''
        输入处理第一段文本
        '''
        short_doc1=input("\033[31minput the first short doc(\'quit\' to quit): ")
        short_doc1 = short_doc1.lower()
        if short_doc1=="quit":
            break
        result_first,num_first=generate_sentence_vector(short_doc1,Model,stopword_set)
        if num_first!= 0:
            result_first=result_first/num_first
        else:
            print("\033[31mFound no words after Token in Doc 1")

        '''
        输入处理第二段文本
        '''
        short_doc2=input("\033[31minput the second short doc(\'quit\' to quit): ")
        short_doc2 = short_doc2.lower()
        if short_doc2=="quit":
            break
        result_second,num_second=generate_sentence_vector(short_doc2,Model,stopword_set)
        if num_second!= 0:
            result_second=result_second/num_second
        else:
            print("\033[31mFound no words after Token in Doc 2")
        '''
        相似度计算
        '''
        cosin_similarity(result_first,result_second)
        # result_first=result_first/np.sqrt(sum(result_first*result_first))
        # result_second=result_second/np.sqrt(sum(result_second*result_second))
        #
        # print("\033[34mnum1 = " + str(num_first)+"\033[34m; num2 = " + str(num_second))
        # print("\033[34mSimilarity = "+ str(sum(result_first*result_second)))


def cal_short_doc_distance_compare(word2vec_Model_1, word2vec_Model_2, stopword_set):
    while (True):
        '''
        输入处理第一段文本
        '''
        short_doc1 = input("\033[31minput the first short doc(\'quit\' to quit): ")
        short_doc1 = short_doc1.lower()
        if short_doc1 == "quit":
            break
        result_first_1, num_first_1 = generate_sentence_vector(short_doc1, word2vec_Model_1, stopword_set)
        result_first_1 = result_first_1 / num_first_1

        result_first_2, num_first_2 = generate_sentence_vector(short_doc1, word2vec_Model_2, stopword_set)
        result_first_2 = result_first_2 / num_first_2


        '''
        输入处理第二段文本
        '''
        short_doc2 = input("\033[31minput the second short doc(\'quit\' to quit): ")
        short_doc2 = short_doc2.lower()
        if short_doc2 == "quit":
            break
        result_second_1, num_second_1 = generate_sentence_vector(short_doc2, word2vec_Model_1, stopword_set)
        result_second_1 = result_second_1 / num_second_1
        result_second_2, num_second_2 = generate_sentence_vector(short_doc2, word2vec_Model_2, stopword_set)
        result_second_2 = result_second_2 / num_second_2

        '''
        相似度计算
        '''
        print("\033[31mModel 1:")
        cosin_similarity(result_first_1,result_second_1)
        print("\033[31mModel 2:")
        cosin_similarity(result_first_2,result_second_2)

if __name__ == '__main__':

    jieba_stop_word_path = "/Users/choukichiou/Desktop/dicts/chinese_stopword.txt"
    stopword_set = loading_stop_word_set(jieba_stop_word_path)
    jieba_userdict_path =  "/Users/choukichiou/Desktop/dicts/jieba_dict.txt"
    loading_jieba_userdict(jieba_userdict_path)
    sc = Connect_to_Spark()

    ''' 
    一模型效果
    '''
    word2vec_Model_path = "/Users/choukichiou/Desktop/Word2VecModels/output_0813"
    word2vec_Model = loading_word2vec_Model(sc,word2vec_Model_path)

    cal_short_doc_distance(word2vec_Model,stopword_set)
    '''     
    两模型效果
    '''
    # word2vec_Model_path = "/Users/choukichiou/Desktop/Word2VecModels/output_0813"
    # word2vec_Model_1 = loading_word2vec_Model(sc, word2vec_Model_path)
    #
    # word2vec_Model_path = "/Users/choukichiou/Desktop/Word2VecModels/output_0813"
    # word2vec_Model_2 = loading_word2vec_Model(sc, word2vec_Model_path)
    #
    # cal_short_doc_distance_compare(word2vec_Model_1,word2vec_Model_2,stopword_set)