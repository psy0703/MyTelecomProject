package com.ng.dataconsumer.util;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {
    public static Properties properties;

    static {
        InputStream is = PropertyUtil.class.getClassLoader().getResourceAsStream("kafka-hbase.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String propName) {
        return properties.getProperty(propName);
    }
}
