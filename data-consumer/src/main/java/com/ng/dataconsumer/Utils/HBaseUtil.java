package com.ng.dataconsumer.Utils;

import com.ng.dataconsumer.coprocessor.CallCoprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 该类主要用于封装一些HBase的常用操作，比如创建命名空间，创建表等等
 */
public class HBaseUtil {
    private static Configuration conf;
    private static Connection conn;
    private static Admin admin;
    private static DecimalFormat decimalFormat = new DecimalFormat("0000");

    static {
        conf = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace){
        if (nameSpaceExist(nameSpace)) return;
        NamespaceDescriptor desc = NamespaceDescriptor.create(nameSpace).build();
        try {
            admin.createNamespace(desc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断命名空间是否存在
     * @param nameSpace
     * @return 存在返回true, 不存在返回false
     */
    private static boolean nameSpaceExist(String nameSpace) {
        try {
            admin.getNamespaceDescriptor(nameSpace);
            return  true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 创建新表
     * @param tableName
     */
    public static void createTable(String tableName){
        //通过kafka-hbase.properties获取HBASE的列族
        byte[] cf = Bytes.toBytes(PropertyUtil.getProperty("hbase.cf"));
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) return;

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            desc.addCoprocessor("com.ng.dataconsumer.com.ng.dataconsumer.coprocessor.CallCoprocessor");
            desc.addFamily(new HColumnDescriptor(cf));
            admin.createTable(desc, getSplitKeys());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 计算Hbase 表单的分区* <p>
     * 0001|
     * 0001|    0002|
     * 0002|    0003|
     * 0003|
     * <p>
    * 0001_xxxx
     * @return
     */
    private static byte[][] getSplitKeys() {
        //获取分区数据
        int regionNum = Integer.parseInt(PropertyUtil.getProperty("hbase.regions"));
        byte[][] keys = new byte[regionNum][];
        //创建分区号
        for (int i = 1; i < regionNum; i++) {
            keys[i - 1] = Bytes.toBytes("000" + i + "|");
        }
        return keys;
    }

    /**
     * 根据表名获取到指定的table
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName){
        try {
            return conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  null;
    }

    /**
     * 根据查询的电话号码和年月计算这行记录的分区
     * @param phone
     * @param year
     * @param month
     * @return
     */
    public static String getRegion(String phone, String year, String month){
        int region = Math.abs((phone + year + "-" + "month").hashCode()) % 6 + 1;//1-6
        return decimalFormat.format(region);
    }
}
