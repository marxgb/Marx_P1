
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object DataAnalysis {
  def main(args: Array[String]):Unit ={

    val conf= new SparkConf().setMaster("local[*]").setAppName("FirstDemo")
    val sc= new SparkContext(conf)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("DROP TABLE IF EXISTS mainTable");
    spark.sql("CREATE TABLE IF NOT EXISTS mainTable(Country STRING, Crop STRING, Measurement STRING, Year INT,  Value DOUBLE) row format delimited fields terminated by ',' TBLPROPERTIES (\"skip.header.line.count\"=\"1\")")
    //.sql("create table IF NOT EXISTS newone(id Int,name String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone")
    //spark.sql("SELECT * FROM newone").show()
    spark.sql("LOAD DATA LOCAL INPATH 'input/cropcom.csv' INTO TABLE mainTable")
    spark.sql("SELECT * FROM mainTable").show()
    spark.sql("SELECT * FROM mainTable WHERE mainTable.Country='JPN'").show()
    spark.sql("SELECT * FROM mainTable WHERE mainTable.Year=2020 AND mainTable.Crop='RICE' SORT BY Country ASC").show(100)
   // spark.sql(sqlText="create table IF NOT EXISTS JapanTable(Country STRING, Crop STRING, Measurement STRING, Year INT) PARTITIONED by (branchid STRING) row format delimited fields terminated by ',' ")
  }
}
