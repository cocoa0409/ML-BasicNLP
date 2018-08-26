import logging

import jieba


def loading_jieba_userdict(path):
    logging.log(logging.CRITICAL, " [Constructing] user dict")
    jieba.load_userdict(path)
    logging.log(logging.CRITICAL, " [Constructing] user dict finish")

def loading_word_set(path):
    stopword_set = set()
    logging.log(logging.CRITICAL, " [Constructing] word dict")
    with open(path, encoding="utf-8", mode="r") as stp:
        for dataline in stp:
            stopword_set.add(dataline[:-1])
    logging.log(logging.CRITICAL, " [Constructing] word dict finish")
    return stopword_set



def LDA_Token(full_doc,Spark_dict_set,space):

    generator = jieba.cut(full_doc,HMM=False)
    result = ""
    for word in generator:
        if word in Spark_dict_set:
            result = result + word + space
    result = result[:-1] + '\n'
    return result

if __name__ == "__main__":

    Spark_Writting_Path ="/Users/choukichiou/Desktop/coop.txt"

    jieba_userdict_path = "/Users/choukichiou/Desktop/dicts_for_LDA/jieba_dict.txt"
    loading_jieba_userdict(jieba_userdict_path)

    Spark_dict_path = "/Users/choukichiou/Desktop/dicts_for_LDA/Spark_dict.txt"
    Spark_dict_set=loading_word_set(Spark_dict_path)

    while(True):
        doc1=input("\033[31minput the first sentence(\'quit\' to quit): \033[34m")
        if doc1 == "quit":
            break
        doc2 = input("\033[31minput the second sentence(\'quit\' to quit): \033[34m")
        if doc2 == "quit":
            break
        with open(Spark_Writting_Path, encoding="utf-8", mode="w") as data:
            data.write(LDA_Token(doc1,Spark_dict_set,"_"))
            data.write(LDA_Token(doc2,Spark_dict_set,"_")[:-1])
            print("\033[31mSuccessfully Writing in File: "+ Spark_Writting_Path)

