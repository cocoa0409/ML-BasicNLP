import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{LocalLDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.RegexTokenizer

import scala.util.control.Breaks
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import breeze.linalg.{argmax, sum, DenseVector => BDV}
import cal_docs_distance_demo.{compute_Hellinger_distance,generate_word_distribution_from_processed_single_test_corpus, corpus_processing, get_topics_for_model, print_top_topic_for_a_doc,single_test_corpus_processing}

object cal_topical_words_demo {


  def print_top_words_for_a_doc(word_distribution:BDV[Double], dict:Array[String], maxTermsPerDoc:Int): Unit ={
    for(i<- 0 to maxTermsPerDoc-1) {
      val index = argmax(word_distribution)
      println("\033[31m===== No.\033[32m" + (i + 1).toString + "\033[31m Topical Word : " + dict(index).toString + " " + word_distribution(index).toString)
      word_distribution(index) = 0
    }
  }




  def main(args: Array[String]) {
    //初始化Spark
    val conf = new SparkConf()
    conf.set("spark.driver.maxResultSize", "4g")
    conf.setMaster("local[*]")
    conf.setAppName("LDA")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //载入模型
    val model_path = "/Users/choukichiou/Desktop/LDAModels/LDA_0814"
    val ldaModel = LocalLDAModel.load(sc,model_path)

    //单文本处理
    val test_corpus_path = "/Users/choukichiou/Desktop/single_doc.txt"
    val dict_path = "/Users/choukichiou/Desktop/dicts_for_LDA/Spark_dict.txt"
    val dict = sc.textFile(dict_path).collect
    val topics = get_topics_for_model(sc,ldaModel,dict_path,maxTermsPerTopic = 5)

    val loop = new Breaks
    loop.breakable(
      while(true){
        print("\033[31mReady to analysis topical words in "+test_corpus_path+"?('y' or 'n'): " )
        val line = Console.readLine
        if(line != "y"){
          loop.break()
        }
        else{
          val test_doc = single_test_corpus_processing(sc,test_corpus_path,dict)
          print_top_topic_for_a_doc(ldaModel.topicDistribution(test_doc),topics,maxTermsPerDoc = 10)
          val word_distribution = generate_word_distribution_from_processed_single_test_corpus(ldaModel,test_doc)
          print_top_words_for_a_doc(word_distribution,dict,maxTermsPerDoc = 20)
        }
      }
    )
  }

}
