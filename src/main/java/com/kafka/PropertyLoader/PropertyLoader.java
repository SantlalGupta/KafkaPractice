package com.kafka.PropertyLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyLoader {
    static InputStream inputStream;
    static Properties properties = new Properties();

    private static  Logger logger = LoggerFactory.getLogger(PropertyLoader.class.getName());

    private static void loadProperty(String propertyFile) {
       try {
           inputStream = PropertyLoader.class.getClassLoader().getResourceAsStream(propertyFile);
           if (inputStream != null) {
               properties.load(inputStream);
           }
       } catch (Exception e){
           logger.error("Exception raised while loading property file : " + propertyFile + " \n" + "Exception : " + e);
       } finally {
           try {
               inputStream.close();
           } catch (IOException e) {
               System.out.println("Exception raised while closing input stream : " + e);
           }
       }
      }

    public static Map<String,String> getMapProperties(String propertyFile){
        loadProperty(propertyFile);
        return new HashMap<String,String>((Map)properties);
    }

   /* public static void main(String[] args) throws IOException {
        System.out.println(PropertyLoader.getMapProperties("config.properties").get(KafkaProperty.BOOTSTRAP_SERVERS));
    }*/
}
