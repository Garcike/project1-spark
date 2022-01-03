package com.Project1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class Handler {

  System.setProperty("hadoop.home.dir", "C:\\hadoop")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  //println("created spark session")

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.sql("SET hive.enforce.bucketing=false")
  spark.sql("SET hive.enforce.sorting=false")
  spark.sql("Create database if not exists Project1")
  spark.sql("use Project1")

  def initialize_table(): Unit ={

    val teams2020 = readFile("C:\\Users\\migue\\OneDrive\\Documents\\GitHub\\project1\\src\\files\\2020_teams.csv", 21, 4)

    spark.sql("Drop table IF EXISTS teams2020")
    spark.sql(s"create table if not exists teams2020(${teams2020(0)(0)} int, ${teams2020(0)(1)} string, ${teams2020(0)(2)} string, ${teams2020(0)(3)} int)")

    (1 until teams2020.length).foreach(x=> spark.sql(s"insert into teams2020 values(${teams2020(x)(0).toInt}, '${teams2020(x)(1)}', '${teams2020(x)(2)}', ${teams2020(x)(3).toInt})"))


    val teams2021 = readFile("C:\\Users\\migue\\OneDrive\\Documents\\GitHub\\project1\\src\\files\\2021_teams.csv", 21, 4)

    spark.sql("Drop table IF EXISTS teams2021")
    spark.sql(s"create table if not exists teams2021(${teams2021(0)(0)} int, ${teams2021(0)(1)} string, ${teams2021(0)(2)} string, ${teams2021(0)(3)} int)")

    (1 until teams2021.length).foreach(x=> spark.sql(s"insert into teams2021 values(${teams2021(x)(0).toInt}, '${teams2021(x)(1)}', '${teams2021(x)(2)}', ${teams2021(x)(3).toInt})"))


    val standings2021 = readFile("C:\\Users\\migue\\OneDrive\\Documents\\GitHub\\project1\\src\\files\\2021_standings.csv", 21, 10)
    spark.sql("Drop table IF EXISTS standings2021")
    spark.sql(s"create table if not exists standings2021(${standings2021(0)(0)} int, ${standings2021(0)(1)} int, ${standings2021(0)(2)} string, ${standings2021(0)(3)} int, ${standings2021(0)(4)} int, ${standings2021(0)(5)} int, ${standings2021(0)(6)} int, ${standings2021(0)(7)} int, ${standings2021(0)(8)} int, ${standings2021(0)(9)} int)")

    (1 until standings2021.length).foreach(x=> spark.sql(s"insert into standings2021 values(${standings2021(x)(0).toInt}, ${standings2021(x)(1).toInt}, '${standings2021(x)(2)}', ${standings2021(x)(3).toInt}, ${standings2021(x)(4).toInt}, ${standings2021(x)(5).toInt}, ${standings2021(x)(6).toInt}, ${standings2021(x)(7).toInt}, ${standings2021(x)(8).toInt}, ${standings2021(x)(9).toInt})"))

    val top6players = readFile("C:\\Users\\migue\\OneDrive\\Documents\\GitHub\\project1\\src\\files\\top6players.csv", 133, 10)

    spark.sql("Drop table IF EXISTS top6players")

    spark.sql(s"create table if not exists top6players(${top6players(0)(0)} int, ${top6players(0)(1)} string, ${top6players(0)(2)} int, ${top6players(0)(3)} int, ${top6players(0)(4)} string, ${top6players(0)(6)} string, ${top6players(0)(7)} int, ${top6players(0)(8)} int, ${top6players(0)(9)} int) PARTITIONED BY (${top6players(0)(5)} int) clustered by (${top6players(0)(0)}) into 10 buckets")

    (1 until top6players.length).foreach(x=> spark.sql(s"insert into top6players partition (${top6players(0)(5)}= ${top6players(x)(5).toInt}) values(${top6players(x)(0).toInt}, '${top6players(x)(1)}', ${top6players(x)(2)}, ${top6players(x)(3).toInt}, '${top6players(x)(4)}', '${top6players(x)(6)}', ${top6players(x)(7).toInt}, ${top6players(x)(8).toInt}, ${top6players(x)(9).toInt})"))

    spark.sql("Drop table IF EXISTS users")
    spark.sql("create table if not exists users(user_name string, user_password string, user_type string)")

  }//end function

  def readFile(file: String, row: Int, col: Int): ArrayBuffer[ArrayBuffer[String]] ={

    val f = scala.io.Source.fromFile(file)
    var index = 0
    val arr: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer.fill(row,col)("")

    for (line <- f.getLines()) {

      val cols = line.split(",").map(_.trim)

      for(c <- 0 until col){ arr(index)(c) = cols(c) }//end for

      index +=1

    }//end for

    arr

  }//end readFile

  def create_user():Unit = {
    println("\n---create an account---")
    println("1. ADMIN")
    println("2. BASIC")
    print("Select user type: ")
    var flag = true
    var input = 0; var user_type = ""
    do{
      try{
        input = scala.io.StdIn.readInt()
        input match{
          case 1 => user_type = "ADMIN"; flag = false
          case 2 => user_type = "BASIC"; flag = false
          case _ => print("Enter a correct option (1-2): ")
        }
      }//end try
      catch{
        case e: NumberFormatException => print("Enter a correct option (1-2): ")
      }

    }while(flag == true)
    print("create an username: "); val name = scala.io.StdIn.readLine()
    print("create a password: " ); val password = scala.io.StdIn.readLine()

    spark.sql(s"insert into users values('${name}', '${password}', '${user_type}')")

  }

  def log_in_verification(name: String, password: String): Int ={

    val df =  spark.sql("select * from users")
    df.where(df("user_name") === name && df("user_password") === password).count().toInt

  }//end log_in

  def get_user_type(name:String, password: String): Int ={
    val df = spark.sql("select * from users")
    df.where(df("user_name") === name && df("user_password") === password && df("user_type") === "BASIC").count().toInt
  }

  def user_menu(user_type: String): Unit ={

    if(user_type == "BASIC"){
      println("1. Update username\n" +
        "2. Update password\n" +
        "3. Show questions\n"  +
        "4. Log out")
    }
    else{
      //user_type = ADMIN
      println("1. Update username\n" +
        "2. Update password\n" +
        "3. Change user's password\n" +
        "4. Delete users\n" +
        "5. Show questions\n"  +
        "6. Log out")
    }


  }//end user_menu

  def change_username(username: String, password:String, user_type:String): Unit ={

    println("\n---Change username---")
    var flag = true
    var new_username: String = ""
    var confirm_new_username: String = ""
    do{
      print("Enter new username: "); new_username = scala.io.StdIn.readLine()
      print("Confirm new username: "); confirm_new_username = scala.io.StdIn.readLine()

      if(new_username == confirm_new_username){
        println("---Username has been changed---")
        flag = false
      }
      else{println("Username does not match. Try again.\n")}

    }while(flag == true)

    spark.sql(s"insert overwrite table users select * from users where user_name != '${username}'")
    spark.sql(s"insert into users values('${new_username}', '${password}', '${user_type}')")

  }//end change_username

  def change_password(username: String, password:String, user_type:String): Unit ={
    println("\n---Change password---")
    var flag = true
    var new_password: String = ""
    var confirm_new_password: String = ""
    do{
      print("Enter new password: "); new_password = scala.io.StdIn.readLine()
      print("Confirm new password: "); confirm_new_password = scala.io.StdIn.readLine()

      if(new_password == confirm_new_password){
        println("---Password has been changed---")
        flag = false
      }
      else{println("Password does not match. Try again.\n")}

    }while(flag == true)

    spark.sql(s"insert overwrite table users select * from users where user_name != '${username}'")
    spark.sql(s"insert into users values('${username}', '${new_password}', '${user_type}')")

  }//end change_password

  def admin_reset_password(): Unit ={
    spark.sql("select * from users order by user_type").show()

    var user_flag = true; var password_flag = true

    var user = ""; var new_password = ""; var confirm_new_password = ""; var user_type = ""

    do {
      print("Which user would you like to change their password: ")
      user = scala.io.StdIn.readLine()

      val df = spark.sql("select * from users")

      if(df.where(df("user_name") === user).count().toInt != 0){
        //get the user type
        val user_t = df.where(df("user_name") === user && df("user_type") === "BASIC").count().toInt

        user_t match{
          case 0 => user_type = "ADMIN"
          case _ => user_type = "BASIC"
        }
        do{
          print("Enter new password: "); new_password = scala.io.StdIn.readLine()
          print("Confirm new password: "); confirm_new_password = scala.io.StdIn.readLine()

          if(new_password == confirm_new_password){
            password_flag = false
          }
          else{println("Password does not match. Try again.\n")}

        }while(password_flag == true)

        user_flag = false
      }
      else{
        println(s"User does not exist: ${user}")
      }//end else
    }while(user_flag == true)

    spark.sql(s"insert overwrite table users select * from users where user_name != '${user}'")
    spark.sql(s"insert into users values('${user}', '${new_password}', '${user_type}')")
    println("---Updated Password---")
    spark.sql("select * from users order by user_type").show()

  }//end admin_changed

  def admin_delete_user(): Unit ={
    spark.sql("select user_name, user_type from users order by user_type").show()

    var flag = true
    do {
      print("Which user would you like to delete: ")
      val deleted_user = scala.io.StdIn.readLine()

      val df = spark.sql("select * from users")

      if(df.where(df("user_name") === deleted_user).count().toInt != 0){
        spark.sql(s"insert overwrite table users select * from users where user_name != '${deleted_user}'")
        flag = false
      }
      else{
        println(s"User does not exist: ${deleted_user}")
      }//end else
    }while(flag == true)
    println("---Updated Users---")
    spark.sql("select user_name, user_type from users order by user_type").show()


  }//end admin_delete_user

  def display_questions(): Unit ={
    //questions

    println("\nTeams in the 2020 season: ")
    spark.sql("select name, founded from teams2020 order by name").show()

    println("Teams in the 2021 season: ")
    spark.sql("select name, founded from teams2021 order by name").show()

    println("***Question 1***")
    println("Which teams got relegated last season?: ")
    spark.sql(relegated2021_query()).show()

    println("***Question 2***")
    println("Which teams got promoted this season?: ")
    spark.sql(promoted2021_query()).show()

    println("2021 Standings")
    spark.sql("select * from standings2021 order by rank").show()
    println("***Question 3***")
    println("In the current 2021 season which teams are fighting to avoid relegation?: ")
    spark.sql(reg_battle_2021()).show()

    println("***Question 4***")
    println("Even the season ended today which teams qualified for the Champions League and Europa League")
    println("Champions League spots")
    spark.sql(champions_league_query()).show()
    println("Europa League spots")
    spark.sql(europa_league_query()).show()

    println("***Question 5***")
    println("For the top 4 teams in the standings, which nationality of players have the most goal contributions?: ")
    question5_view()

    println("***Question 6***")
    println("How many games have been postponed so far in the 2021 season because of COVID or schedule congestion: ")
    spark.sql(pp_games_query()).show()

    println("***Question 7***")
    println("Do Chelsea's defenders have more goal contributions than their attackers")
    spark.sql(team_gc_by_position(49)).show()



  }//end display_questions

  def promoted2021_query():String ={
    """
      |select teams2021.name as promoted_teams
      |from teams2021
      |left outer join teams2020 on (teams2021.name = teams2020.name)
      |where teams2020.name is null
      |""".stripMargin
  }

  def relegated2021_query():String ={
    """
      |select teams2020.name as relegated_teams
      |from teams2021
      |right outer join teams2020 on (teams2021.name = teams2020.name)
      |where teams2021.name is null
      |""".stripMargin
  }

  def reg_battle_2021():String={
    """
      |select name, points
      |from standings2021
      |where rank > 5
      |order by points
      |limit 5
      |""".stripMargin
  }

  def question3(): String = {
    """
      |select nationality, sum(goals), sum(assists), sum(goals + assists) as goal_contributions
      |from top6players
      |group by nationality
      |order by goal_contributions desc
      |limit 5
      |""".stripMargin
  }

  def question5_view(): Unit ={

    spark.sql("Drop view IF EXISTS top4_gc")
    val view_query =
      """
        |create view if not exists top4_gc as
        |select t.team, t.name, t.nationality, t.appearances, t.goals, t.assists
        |from top6players t
        |inner join standings2021 s on (t.team = s.id)
        |where s.rank < 5
        |""".stripMargin

    spark.sql(view_query)
    val query =
      """
        |select nationality, sum(goals), sum(assists), sum(goals + assists) as goal_contributions
        |from top4_gc
        |group by nationality
        |order by goal_contributions desc
        |limit 5
        |""".stripMargin

    spark.sql(query).show()
  }

  def pp_games_query(): String ={

    //as of 1/2/2022
    val games_that_were_scheduled = (21 * 20) - 8

    val query =
      s"""
         |select ((${games_that_were_scheduled} - sum(played))/2) as games_pp
         |from standings2021
         |""".stripMargin

    query
  }

  def champions_league_query(): String ={

    """
      |select rank, name, points
      |from standings2021
      |where rank < 5
      |order by rank
      |""".stripMargin

  }

  def europa_league_query(): String ={

    """
      |select rank, name, points
      |from standings2021
      |where rank > 4 and rank < 7
      |order by rank
      |""".stripMargin

  }

  def team_gc_by_position(team: Int): String ={

    s"""
       |select position, sum(goals), sum(assists), sum(goals + assists) as goal_contributions
       |from top6players
       |where team = ${team}
       |group by position
       |order by goal_contributions desc
       |""".stripMargin

  }//end unit

  def spark_close(): Unit ={spark.close()}

}//end class
