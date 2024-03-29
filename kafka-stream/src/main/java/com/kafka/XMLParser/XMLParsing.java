package com.kafka.XMLParser;

import org.w3c.dom.*;

import javax.xml.parsers.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class XMLParsing {

    public static List<Student> getStudentList(String xml) {

        return xmlParser(xml);
    }

    private static List<Student> xmlParser(String xml) {
        List studList = new ArrayList<Student>();
        try {
            DocumentBuilderFactory factory =
                    DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();

            StringBuilder xmlStringBuilder = new StringBuilder();
            xmlStringBuilder.append(xml);
            ByteArrayInputStream input = new ByteArrayInputStream(
                    xmlStringBuilder.toString().getBytes("UTF-8"));
            Document doc = builder.parse(input);

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
            NodeList nList = doc.getElementsByTagName("student");
            System.out.println("----------------------------");

            for (int temp = 0; temp < nList.getLength(); temp++) {
                Student s = new Student();
                Node nNode = nList.item(temp);
                System.out.println("\nCurrent Element :" + nNode.getNodeName());

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    System.out.println("Student roll no : "
                            + eElement.getAttribute("rollno"));
                    s.setRollno(Integer.parseInt(eElement.getAttribute("rollno")));

                    System.out.println("First Name : " + eElement.getElementsByTagName("firstname").item(0).getTextContent());
                    System.out.println("Last Name : " + eElement.getElementsByTagName("lastname").item(0).getTextContent());
                    System.out.println("Nick Name : " + eElement.getElementsByTagName("nickname").item(0).getTextContent());
                    System.out.println("Marks : " + eElement.getElementsByTagName("marks").item(0).getTextContent());

                    s.setFirstname(eElement.getElementsByTagName("firstname").item(0).getTextContent());
                    s.setLastname(eElement.getElementsByTagName("lastname").item(0).getTextContent());
                    s.setNickname(eElement.getElementsByTagName("nickname").item(0).getTextContent());
                    s.setMarks(Integer.parseInt(eElement.getElementsByTagName("marks").item(0).getTextContent()));

                    studList.add(s);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return studList;
    }

    public static void main(String[] args) {
        XMLParsing x = new XMLParsing();
        String s = "<?xml version = \"1.0\"?> <class>    <student rollno = \"393\">       <firstname>dinkar</firstname>       <lastname>kad</lastname>       <nickname>dinkar</nickname>       <marks>85</marks>    </student> </class>";
        List<Student> studList = getStudentList(s);
        for(Student stud : studList) {
            System.out.println(stud.getFirstname() + " \n" + stud.getLastname() + "\n" + stud.getNickname() + "\n" + stud.getRollno());
        }

    }
}
