package com.ng.dataconsumer.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 该类是一个工具类, 可以读取配置文件: kafka-hbase.properties中属性的值.
 * 通过方法getProperty() 可以获取你需要的属性的值
 */
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

    /**
     * 根据指定的属性名返回对应的属性值
     * @param propName
     * @return
     */
    public static  String getProperty(String propName){
        return properties.getProperty(propName);
    }
}
