
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.sql.SQLContext


object LDA {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    //        conf.set("spark.driver.maxResultSize", "4g")
    //        conf.setMaster("local[*]")
    conf.setAppName("LDA")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val corpus_body_path = "/user/root/wangzhaoji/input/Spark_detail_0814.txt"
    val dict_path = "/user/root/wangzhaoji/input/dicts_for_LDA/Spark_dict.txt"
    val model_save_path = "/user/root/wangzhaoji/outputlda"


    val corpus_body = sc.textFile(corpus_body_path).filter(_.trim != "")
    val corpus_df = corpus_body.zipWithIndex.toDF("corpus", "id")
    import org.apache.spark.ml.feature.RegexTokenizer
    val tokenizer = new RegexTokenizer().setPattern("_").setInputCol("corpus").setOutputCol("tokens")
    val tokenized_df = tokenizer.transform(corpus_df)
    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
    //加载自定义词典
    val dict = sc.textFile(dict_path).collect
    val vectorizer = new CountVectorizerModel(dict).setInputCol("tokens").setOutputCol("features")
    val countVectors = vectorizer.transform(tokenized_df).select("id", "features")

    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.Row
    val lda_countVector = countVectors.map { case Row(id: Long, countVector: Vector) => (id, org.apache.spark.mllib.linalg.Vectors.fromML(countVector.toSparse)) }.rdd

    val numTopics = 1000
    val MaxIteration = 100

    import org.apache.spark.mllib.clustering.LDA

    // Set LDA params
    val lda = new LDA().setK(numTopics).setMaxIterations(MaxIteration).setDocConcentration(-1).setTopicConcentration(-1) // use default values


    val ldaModel = lda.run(lda_countVector).asInstanceOf[DistributedLDAModel]
    val local_ldaModel = ldaModel.toLocal
    local_ldaModel.save(sc, model_save_path)


  }
}