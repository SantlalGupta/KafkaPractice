package com.kafka.XMLParser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JavaJdbc {

    /**
     * Table create statement
     *  create table student (  rollno int, firstname varchar(40), lastname varchar(30), nickname varchar(30), marks int);
     * @param studList
     */

    public  static void insertRecordIntoMysql(List<Student> studList){
       Connection conn=null;
        try {
           Class.forName("com.mysql.jdbc.Driver");
           conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "M_zaq1xsw2");
           String query = " insert into Student (rollno,firstname,lastname,nickname,marks)"
                   + " values (?, ?, ?, ?, ?)";

           // create the mysql insert preparedstatement
           PreparedStatement preparedStmt = conn.prepareStatement(query);
            int i =0;
           for (Student stud:studList) {
               preparedStmt.setInt(1, stud.getRollno());
               preparedStmt.setString(2, stud.getFirstname());
               preparedStmt.setString(3, stud.getLastname());
               preparedStmt.setString(4, stud.getNickname());
               preparedStmt.setInt(5, stud.getMarks());

               preparedStmt.addBatch();
                i=i+1;
               if (i % 1000 == 0 || i == studList.size()) {
                   preparedStmt.executeBatch(); // Execute every 1000 items.
               }
           }

       } catch (Exception e) {
           e.printStackTrace();
       } finally {
            try {
                if (conn != null)
                    conn.close();
            }catch (SQLException s) {
                s.printStackTrace();
            }
       }
    }

    public static void main(String[] args) {
         Student stud = new Student();
           stud.setMarks(50);
           stud.setLastname("a");
           stud.setFirstname("b");
           stud.setNickname("c");
           stud.setRollno(11);
           List st = new ArrayList<Student>();
           st.add(stud);
        JavaJdbc.insertRecordIntoMysql(st);
        System.out.println("record inserted");
    }
}
