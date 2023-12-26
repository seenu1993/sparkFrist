
package object projectwork {

  import org.apache.spark.SparkContext // rdd
  import org.apache.spark.sql.SparkSession // dataframe
  import org.apache.spark.SparkConf
  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.types.IntegerType
  import org.apache.spark.sql.functions.upper
  import org.apache.spark.sql.catalyst.expressions.Upper
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions.Window
  import scala.io.Source

  object projectwork {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._

      val df = spark.read.format("avro").option("header", "true").load("C:\\Users\\91701\\Desktop\\ptojectwork\\projectsample.avro")

      df.show()

    }}}
