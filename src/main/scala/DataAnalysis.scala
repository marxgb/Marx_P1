
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object DataAnalysis {
  def main(args: Array[String]): Unit = {

    /*val conf= new SparkConf().setMaster("local[*]").setAppName("FirstDemo")
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
    */
    var option = ""
    var logIn = "false"
    var userCredentials = new Array[String](3)
    while (logIn == "false") {
      println("Do you want to login or create a new user?" + '\n' + "(1) Login" + '\n' + "(2) Create New User")
      option = scala.io.StdIn.readLine()
      if (option == "1") {
        userCredentials = loginUser()
        logIn = userCredentials(3)
      }
      else if (option == "2") {
        userCredentials = createUser()
        logIn = userCredentials(3)
      }
      else {
        println("Invalid input!")
      }
    }

    var exit = false
    var option2 =""
    while(exit == false){
      println("Select an option:" + '\n' + "(1) Analyze data" + '\n' + "(2) Update user credentials" + '\n' + "(3) Show current user credentials")
      option2 = scala.io.StdIn.readLine()

      if(option2=="1"){

      }
      else if (option2 == "2"){
        updateUserCredentials(userCredentials)
      }
      else if (option2 == "3"){
        println("Current user is: " + userCredentials(0) + " and current user has " + userCredentials(2) + " access")
      }
      else
        println("Invalid input!")

      exit = true

    }
  }
  //METHOD FOR LOGGING IN A USER
  def loginUser(): Array[String] = {
    var arrayCredentials = new Array[String](4)
    arrayCredentials (3) = "false"
    println("Enter your username: ")
    arrayCredentials(0) = scala.io.StdIn.readLine()
    println("Enter your password: ")
    arrayCredentials(1) = scala.io.StdIn.readLine()
    arrayCredentials(2) = "basic"
    arrayCredentials(3) = "true"
    println("Log In sucessful!")
    return arrayCredentials
  }

  //METHOD FOR CREATING  A USER
  def createUser(): Array[String] = {
    var arrayCredentials = new Array[String](4)
    println("Enter your username: ")
    arrayCredentials(0) = scala.io.StdIn.readLine()
    println("Enter your password: ")
    arrayCredentials(1) = scala.io.StdIn.readLine()
    var userCredential = "false"
    while (userCredential == "false") {
      println("Do you want a basic user or an admin user?")
      arrayCredentials(2) = scala.io.StdIn.readLine()
      if (arrayCredentials(2) == "basic" || arrayCredentials(2) == "admin")
        userCredential = "true"
      else
        println("Invalid input!")
    }
    arrayCredentials(3) = "true"
    return arrayCredentials
  }

  //METHOD FOR UPDATING USER CREDENTIALS
  def updateUserCredentials(x : Array[String]): Array[String] ={
    var arrayCredentials = x
    var exit = false
    var option = ""
    while(exit == false){
      println("Updating your user credentials"+ '\n' +"(1) Update username only" + '\n' + "(2) Update password only" + '\n' + "(3) Update username and password")
      option = scala.io.StdIn.readLine()
      if(option == "1"){
        println("Enter new username: ")
        arrayCredentials(0) = scala.io.StdIn.readLine()
        println("Successful updated username to " + arrayCredentials(0))
        exit = true
      }
      else if(option == "2"){
        println("Enter new password: ")
        arrayCredentials(1) = scala.io.StdIn.readLine()
        println("Successful updated password!")
        exit = true
      }
      else if(option == "3"){
        println("Enter new username: ")
        arrayCredentials(0) = scala.io.StdIn.readLine()
        println("Successful updated username to " + arrayCredentials(0) + '\n' + "Enter new password: ")
        arrayCredentials(1) = scala.io.StdIn.readLine()
        println("Successful updated password!")
        exit = true
      }
      else
        println("Invalid input!")
    }
    exit = false
    while (exit == false) {
      println("Are you sure you want to save the updates? " + '\n' + "(1) Yes " + '\n' + "(2) No")
      if (option == "1") {
        exit = true
        println("Changes saved!")
      }
      else if (option == "2") {
        arrayCredentials = x
        exit = true
      }
      else
        println("Invalid input!")
    }
    return arrayCredentials
  }

}