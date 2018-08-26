
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.Word2Vec

object Word2Vec{
  def main(args: Array[String]) {
    val conf = new SparkConf()
//    conf.set("spark.driver.maxResultSize", "4g")
//    conf.setMaster("local[*]")
    conf.setAppName("word2vec")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/user/root/wangzhaoji/input/detail_0810.txt")
//    val input = sc.textFile("/Users/choukichiou/Desktop/detail.txt")
    val inputSeq = input.map(line => line.split("_").toSeq)
    val word2vec = new Word2Vec()
        .setVectorSize(200)
        .setMinCount(100)
        .setNumPartitions(100)
        .setNumIterations(1)
    val word2vecModel = word2vec.fit(inputSeq)
    word2vecModel.save(sc, "/user/root/wangzhaoji/output")
//    word2vecModel.save(sc,"/Users/choukichiou/Desktop/model")
  }
}