import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{ LocalLDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.RegexTokenizer

import scala.util.control.Breaks
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import breeze.linalg.{argmax, sum, DenseVector => BDV}


object cal_docs_distance_demo {

  def corpus_processing(sc:SparkContext,corpus_path:String,dict:Array[String]):RDD[(Long, org.apache.spark.mllib.linalg.Vector)]={

    val corpus_body =sc.textFile(corpus_path).filter(_.trim != "")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val corpus_df = corpus_body.zipWithIndex.toDF("corpus", "id")
    val tokenizer = new RegexTokenizer().setPattern("_").setInputCol("corpus").setOutputCol("tokens")
    val tokenized_df = tokenizer.transform(corpus_df)

    //加载自定义词典
    val vectorizer = new CountVectorizerModel(dict).setInputCol("tokens").setOutputCol("features")
    val countVectors = vectorizer.transform(tokenized_df).select("id", "features")

    import org.apache.spark.ml.linalg.Vector
    val lda_countVector = countVectors.map { case Row(id: Long, countVector: Vector) =>(id,org.apache.spark.mllib.linalg.Vectors.fromML(countVector.toSparse)) }.rdd

    return lda_countVector
  }

  def single_test_corpus_processing(sc:SparkContext,test_corpus_path:String,dict:Array[String]):org.apache.spark.mllib.linalg.Vector={

    val lda_countVector = corpus_processing(sc,test_corpus_path,dict)

    val test_doc_vector = lda_countVector.collect.last._2

    return test_doc_vector
  }

//  def double_test_corpus_processing(sc:SparkContext,test_corpus_path:String,dict_path:String):(org.apache.spark.mllib.linalg.Vector,org.apache.spark.mllib.linalg.Vector) ={
//    val lda_countVector = corpus_processing(sc,test_corpus_path,dict_path)
//
//    val first_doc_vector = lda_countVector.collect.head._2
//    val second_doc_vector = lda_countVector.collect.last._2
//
//    return (first_doc_vector,second_doc_vector)
//  }

  def print_topics_for_model(sc: SparkContext,ldaModel:LocalLDAModel,dict_path:String,maxTermsPerTopic:Int){
    val vocabList = sc.textFile(dict_path).collect
    //Array[String]
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic)
    //Array[(Array[Int], Array[Double])]
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabList(_)).zip(termWeights)
    }

    val numTopics = ldaModel.k

    println(s"$numTopics topics:")
    //    writer.write(s"$numTopics topics:"+"\n")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      //      writer.write(s"TOPIC $i"+"\n")
      topic.foreach { case (term, weight) => println(s"$term\t$weight") }
      //      topic.foreach { case (term, weight) => writer.write(s"$term\t$weight"+"\n") }
      println(s"==========")
      //      writer.write(s"=========="+"\n")
    }
  }

  def get_topics_for_model(sc: SparkContext,ldaModel:LocalLDAModel,dict_path:String,maxTermsPerTopic:Int):Array[Array[(String, Double)]]={
    val vocabList = sc.textFile(dict_path).collect
    //Array[String]
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic)
    //Array[(Array[Int], Array[Double])]
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabList(_)).zip(termWeights)
    }
    return topics
  }



  def generate_word_distribution_from_processed_single_test_corpus(ldaModel:LocalLDAModel,test_doc_vector:org.apache.spark.mllib.linalg.Vector):breeze.linalg.DenseVector[Double]={
    val TopicDistribution = ldaModel.topicDistribution(test_doc_vector)
    val TopicDistribution_BDV =  new BDV(TopicDistribution.toDense.values)

    val TopicsMatrix = ldaModel.topicsMatrix

    val rows = ldaModel.vocabSize
    val cols = ldaModel.k
    val TopicsMatrix_BDM = breeze.linalg.DenseMatrix.zeros[Double](rows,cols)

    for( i <- 0 to ldaModel.k - 1){
      var result = 0.0
      for( j <- 0 to ldaModel.vocabSize - 1){
        result = result + TopicsMatrix(j,i)
      }
      for( j <- 0 to ldaModel.vocabSize - 1){
        TopicsMatrix_BDM(j,i) = TopicsMatrix(j,i)/result
      }
    }

    val WordDistribution = TopicsMatrix_BDM * TopicDistribution_BDV
    return WordDistribution
  }


  def compute_Hellinger_distance(doc_1_TopicDistribution:org.apache.spark.mllib.linalg.Vector,doc_2_TopicDistribution:org.apache.spark.mllib.linalg.Vector): Double ={
    val doc1_BDV =  new BDV(doc_1_TopicDistribution.toDense.values)
    val doc2_BDV =  new BDV(doc_2_TopicDistribution.toDense.values)

    assert(doc1_BDV.length == doc2_BDV.length)

    for (i <- 0 to doc1_BDV.length-1){
      doc1_BDV(i) = math.sqrt(doc1_BDV(i))
      doc2_BDV(i) = math.sqrt(doc2_BDV(i))
    }

    val diff = doc1_BDV - doc2_BDV

    val Hellinger_distance = math.sqrt(sum(diff*diff)/2)

    return Hellinger_distance
  }

  def print_top_topic_for_a_doc(doc_1_TopicDistribution:org.apache.spark.mllib.linalg.Vector,topics:Array[Array[(String, Double)]],maxTermsPerDoc:Int): Unit ={
    for(i<- 0 to maxTermsPerDoc-1){
      val doc_1 = new BDV(doc_1_TopicDistribution.toDense.values)
      val index = argmax(doc_1)
      val topic = topics(index)
      println("\033[31m===== No.\033[32m"+(i+1).toString+"\033[31m Topic =====(Topic "+index.toString+": "+ (doc_1(index)*doc_1(index)).toString+")")
      topic.foreach{case (term, weight) => print(s"\033[34m[$term:$weight] ")}
      println(" ")
      doc_1(index)=0
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
    val test_corpus_path = "/Users/choukichiou/Desktop/coop.txt"
    val dict_path = "/Users/choukichiou/Desktop/dicts_for_LDA/Spark_dict.txt"
    val dict = sc.textFile(dict_path).collect
    val topics = get_topics_for_model(sc,ldaModel,dict_path,maxTermsPerTopic = 5)

    val loop = new Breaks
    loop.breakable(
      while(true){
        print("\033[31mReady to analysis the distance of doc 1 and doc 2 in "+test_corpus_path+"?('y' or 'n'): " )
        val line = Console.readLine
        if(line != "y"){
          loop.break()
        }
        else{

          val test_docs = corpus_processing(sc,test_corpus_path,dict)

          val test_docs_TopicDistribution = ldaModel.topicDistributions(test_docs).collect()

          val doc_1_TopicDistribution = test_docs_TopicDistribution(0)._2
          val doc_2_TopicDistribution = test_docs_TopicDistribution(1)._2

          val Hellinger_distance = compute_Hellinger_distance(doc_1_TopicDistribution,doc_2_TopicDistribution)

          println("\033[31mDoc 1 extracted Infomation:")
          print_top_topic_for_a_doc(doc_1_TopicDistribution,topics,maxTermsPerDoc = 10)
          println("\033[31mDoc 2 extracted Infomation:")
          print_top_topic_for_a_doc(doc_2_TopicDistribution,topics,maxTermsPerDoc = 10)
          println("\033[41mHellinger_distance = " + Hellinger_distance.toString+"\033[31m")
        }

      }


    )

//    print_topics_for_model(sc,ldaModel,dict_path,maxTermsPerTopic = 10)
    // 验证集

  }
}
