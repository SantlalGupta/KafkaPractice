package com.kafka.PropertyLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyLoader {
    static InputStream inputStream;
    static Properties properties = new Properties();

    private static void loadProperty(String propertyFile) {
       try {
           inputStream = PropertyLoader.class.getClassLoader().getResourceAsStream(propertyFile);
           if (inputStream != null) {
               properties.load(inputStream);
           }
       } catch (Exception e){
           System.out.println("Exception : " + e);
       } finally {
           try {
               inputStream.close();
           } catch (IOException e) {
               System.out.println("Exception raised while closing input stream : " + e);
           }
       }
      }

    public static Properties getProperties(String propertyFile){
        loadProperty(propertyFile);
        return properties;
    }

    /*public static void main(String[] args) throws IOException {
        System.out.println(PropertyLoader.getProperties("config.properties").getProperty(KafkaProperty.BOOTSTRAP_SERVERS));
    }*/

}
