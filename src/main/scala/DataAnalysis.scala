import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager, SQLException}

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://localhost:3306/usrpwproject1"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "coff33addicT"
    var connection:Connection = null
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    /*
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
    //spark.sql("DROP TABLE IF EXISTS mainTable");
    //spark.sql("CREATE TABLE IF NOT EXISTS mainTable(Country STRING, Crop STRING, Measurement STRING, Year INT,  Value DOUBLE) row format delimited fields terminated by ',' TBLPROPERTIES (\"skip.header.line.count\"=\"1\")")
    //.sql("create table IF NOT EXISTS newone(id Int,name String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone")
    //spark.sql("SELECT * FROM newone").show()
    //spark.sql("LOAD DATA LOCAL INPATH 'input/cropcom.csv' INTO TABLE mainTable")
    //spark.sql("SELECT * FROM mainTable").show()
    //spark.sql("SELECT * FROM mainTable WHERE mainTable.Country='JPN'").show()
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
        userCredentials = loginUser(connection)
        logIn = "true"
      }
      else if (option == "2") {
        userCredentials = createUser(connection)
        logIn = "true"
      }
      else {
        println("Invalid input!")
      }
    }

    var exit = false
    var option2 =""
    while(exit == false){
      println("Select an option:" + '\n' + "(1) Analyze data" + '\n' + "(2) Update user credentials" + '\n' + "(3) Show current user credentials" + '\n' + "(4) Exit")
      option2 = scala.io.StdIn.readLine()

      if(option2=="1"){

      }
      else if (option2 == "2"){
        updateUserCredentials(userCredentials, connection)}
      else if (option2 == "3"){
        println("Current user is: " + userCredentials(0) + " and current user has " + userCredentials(2) + " access")}
      else if (option2 == "4")
        exit = true
      else
        println("Invalid input!")


    }


}
  /////////////////////////////////METHODS ARE HERE////////////////////////////////////////////////////
  //METHOD FOR LOGGING IN A USER
  def loginUser(connection: Connection): Array[String] = {
    var arrayCredentials = new Array[String](3)
    var checkIfSuccess = false
    while(checkIfSuccess == false) {
      println("Enter your username: ")
      arrayCredentials(0) = scala.io.StdIn.readLine()
      println("Enter your password: ")
      arrayCredentials(1) = scala.io.StdIn.readLine()

      var pstmt = connection.prepareStatement("SELECT username, password, level FROM usrpw WHERE username=? AND password=?")
      println(arrayCredentials(0) + " " + arrayCredentials(1))
      pstmt.setString(1, arrayCredentials(0))
      pstmt.setString(2, arrayCredentials(1))
      var rs = pstmt.executeQuery()

      while (rs.next) {
        val ex = rs.getString("username")
        val ex1 = rs.getString("password")
        val ex2 = rs.getString("level")
        arrayCredentials(2) = ex2
        checkIfSuccess = true
      }
      if(checkIfSuccess == false)
        println("Login failed! ")
    }
    return arrayCredentials
  }

  //METHOD FOR CREATING  A USER
  def createUser(connection: Connection): Array[String] = {
    var arrayCredentials = new Array[String](3)
    println("Enter your username: ")
    arrayCredentials(0) = scala.io.StdIn.readLine()
    println("Enter your password: ")
    arrayCredentials(1) = scala.io.StdIn.readLine()
    var userCredential = "false"
    while (userCredential == "false"){
      println("Do you want to create a basic or admin user?" + '\n' + "(1) Basic" + '\n' + "(2) Admin")
      arrayCredentials(2) = scala.io.StdIn.readLine()
      if(arrayCredentials(2) == "1") {
        arrayCredentials(2) = "basic"
        userCredential = "true"}
      else if( arrayCredentials(2) == "2") {
        arrayCredentials(2) ="admin"
        userCredential = "true"}
      else
        println("Invalid input!")
    }
    var pstmt = connection.prepareStatement("INSERT INTO usrpw(username,password,level) VALUES(?,?,?)")
    try{
      pstmt.setString(1, arrayCredentials(0))
      pstmt.setString(2, arrayCredentials(1))
      pstmt.setString(3, arrayCredentials(2))
      pstmt.executeUpdate
      println("USER CREATED")
    }
    catch {
      case e: IllegalArgumentException => println("ERROR CREATING USER")
    }
    return arrayCredentials
  }

  //METHOD FOR UPDATING USER CREDENTIALS
  def updateUserCredentials(x : Array[String], connection: Connection): Array[String] ={
    val oldUsername = x(0)
    val oldPassword = x(1)
    var arrayCredentials = x
    var exit = false
    var option = ""
    while(exit == false){
      println("Updating your user credentials"+ '\n' +"(1) Update username only" + '\n' + "(2) Update password only" + '\n' + "(3) Update username and password")
      option = scala.io.StdIn.readLine()
      if(option == "1"){
        println("Enter new username: ")
        arrayCredentials(0) = scala.io.StdIn.readLine()
        exit = true }
      else if(option == "2"){
        println("Enter new password: ")
        arrayCredentials(1) = scala.io.StdIn.readLine()
        exit = true }
      else if(option == "3"){
        println("Enter new username: ")
        arrayCredentials(0) = scala.io.StdIn.readLine()
        println("Enter new password: ")
        arrayCredentials(1) = scala.io.StdIn.readLine()
        exit = true }
      else
        println("Invalid input!")
    }
    var pstmt = connection.prepareStatement("UPDATE usrpw SET username=? , password =? WHERE username=? AND password =?")
    try{
      pstmt.setString(1, arrayCredentials(0))
      pstmt.setString(2, arrayCredentials(1))
      pstmt.setString(3, oldUsername)
      pstmt.setString(4, oldPassword)
      pstmt.executeUpdate
      println("Updated user credentials!")

    }
    catch {
      case e: IllegalArgumentException => println("ERROR CREATING USER")
    }
    return arrayCredentials
  }

}