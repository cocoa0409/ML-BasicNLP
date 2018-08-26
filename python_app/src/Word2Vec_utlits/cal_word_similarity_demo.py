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


def cal_word_similarity(Model):
    while(True):
        '''
        输入单词
        '''
        word=input("\033[31minput a word(\'quit\' to quit): \033[34m")
        if word=="quit":
            break
        '''
        findSynonyms
        '''
        try:
            result_list=list(Model.findSynonyms(word,20))
            for result in result_list:
                print(result)
        except:
            print("\033[32mWord \'" + word + "\' is not in vocabulary")

def cal_word_similarity_compare(Model1,Model2):
    while(True):
        '''
        输入单词
        '''
        word=input("\033[31minput the first short doc(\'quit\' to quit): \033[34m")
        if word=="quit":
            break
        '''
        findSynonyms
        '''
        flag = False
        try:
            result_list1 = list(Model1.findSynonyms(word,20))
        except:
            print("\033[32mWord \'" + word + "\' is not in vocabulary 1\033[34m")
            flag = True
        try:
            result_list2 = list(Model2.findSynonyms(word, 20))
        except:
            print("\033[32mWord \'" + word + "\' is not in vocabulary 2\033[34m")
            flag = True

        if flag == True:
            continue

        for i in range(0,20):
            print(str(result_list1[i])+'\t'+str(result_list2[i]))



if __name__ == '__main__':
    '''
    一个模型
    '''
    # word2vec_Model_path = "/Users/choukichiou/Desktop/full_model/output_0807"
    # sc = Connect_to_Spark()
    # word2vec_Model = loading_word2vec_Model(sc,word2vec_Model_path)
    # cal_word_similarity(word2vec_Model)

    '''
    两个模型
    '''
    sc = Connect_to_Spark()
    word2vec_Model_path = "/Users/choukichiou/Desktop/Word2VecModels/output_0813"
    word2vec_Model_1 = loading_word2vec_Model(sc,word2vec_Model_path)

    word2vec_Model_path = "/Users/choukichiou/Desktop/Word2VecModels/output_0808"
    word2vec_Model_2 = loading_word2vec_Model(sc,word2vec_Model_path)
    cal_word_similarity_compare(word2vec_Model_1,word2vec_Model_2)



'''
百度云API
from aip import AipNlp

APP_ID = '58a569015c78464a92e84b532329e9e9'
API_KEY = 'kk2IKGelnD1w6wjzHSygsijO'
SECRET_KEY = 'jSTFyZ5Eu0M4HTmj1sY8C59qi41Wft6W'

client = AipNlp(APP_ID, API_KEY, SECRET_KEY)

'''