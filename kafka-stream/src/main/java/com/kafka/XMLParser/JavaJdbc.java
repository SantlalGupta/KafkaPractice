package com.kafka.XMLParser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JavaJdbc {

    public  static void insertRecordIntoMysql(Student stud){
       try {
           Class.forName("com.mysql.jdbc.Driver");
           Connection conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "M_zaq1xsw2");
           String query = " insert into Student (rollno,firstname,lastname,nickname,marks)"
                   + " values (?, ?, ?, ?, ?)";

           // create the mysql insert preparedstatement
           PreparedStatement preparedStmt = conn.prepareStatement(query);
           preparedStmt.setInt (1, stud.getRollno());
           preparedStmt.setString (2, stud.getFirstname());
           preparedStmt.setString   (3, stud.getLastname());
           preparedStmt.setString(4, stud.getNickname());
           preparedStmt.setInt    (5, stud.getMarks());

           // execute the preparedstatement
           preparedStmt.execute();

           conn.close();

       } catch (Exception e) {
           e.printStackTrace();
       }
    }

    public static void main(String[] args) {
         Student stud = new Student();
           stud.setMarks(50);
           stud.setLastname("a");
           stud.setFirstname("b");
           stud.setNickname("c");
           stud.setRollno(11);
        JavaJdbc.insertRecordIntoMysql(stud);
        System.out.println("record inserted");
    }
}
