package com.Project1

import scala.util.control.Breaks.{break, breakable}

object main extends App{

  val h = new Handler()

  //h.initialize_table()

  println("---Project 1---")
  var running = true

  while(running == true) {

    println("\n1. Create an account")
    println("2. Log in")
    println("3. Exit")
    print("Select an option: ")

    breakable {
      var flag1 = true
      do {
        try {
          val input = scala.io.StdIn.readInt()
          input match {
            case 1 => h.create_user(); flag1 = false
            case 2 => flag1 = false
            case 3 => running = false; break();
            case _ => print("Enter a correct option (1 - 2): ")
          } //end match
        } //end try
        catch {
          case e: NumberFormatException => print("Enter a correct option (1-2): ")
        } //end catch
      } while (flag1 == true)

      //log in
      println("\n---Log in---")

      var flag2 = true
      var username: String = ""
      var password: String = ""

      do {
        print("\nEnter username: ");
        username = scala.io.StdIn.readLine()
        print("Enter password: ");
        password = scala.io.StdIn.readLine()

        val valid = h.log_in_verification(username, password)
        valid match {
          case 1 => flag2 = false
          case _ => println("Error! Your username or password was incorrect.\nPlease try again")
        }
      } while (flag2 == true)

      println(s"\n---Welcome ${username}---")

      val user_type: Int = h.get_user_type(username, password)

      if (user_type == 0) {
        var flag3 = true
        do{
          h.user_menu("ADMIN")
          print("Select an option: ")
          //do{
          try{
            val user_input = scala.io.StdIn.readInt()
            user_input match{
              case 1 => h.change_username(username, password, "ADMIN"); break()
              case 2 => h.change_password(username, password, "ADMIN"); break()
              case 3 => h.admin_reset_password()
              case 4 => h.admin_delete_user()
              case 5 => h.display_questions()
              case 6 => println("\n---Logout---"); break()
            }//end match
          }//end try
          catch{
            case e:NumberFormatException => print("Pick a correct option (1-5): ")
          }

        }while(flag3 == true)
      }
      else {
        var flag3 = true
        do{
          h.user_menu("BASIC")
          print("Select an option: ")
          try {
            val user_input = scala.io.StdIn.readInt()
            user_input match {
              case 1 => h.change_username(username, password, "BASIC"); break()
              case 2 => h.change_password(username, password, "BASIC"); break()
              case 3 => h.display_questions(); //flag3 = false
              case 4 => println("\n---Log out---"); break();
              case _ => print("Enter a correct option (1-4): ")
            } //end match
          }
          catch {
            case e: NumberFormatException => print("Enter a correct option (1-4): ")
          }

        } while (flag3 == true)

      }//end else


    }//end breakable

  }//end main loop

  println("\n---The End---")

  //spark.close()
  h.spark_close()


}//end main
