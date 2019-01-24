package com.ng.dataconsumer.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.text.DecimalFormat;

public class HBaseUtil {
    private static Configuration conf;
    private static Connection conn;

    private static Admin admin;
    private static DecimalFormat decimalFormat = new DecimalFormat("0000");

    static {

        try {
            conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     *
     * @param ns
     */
    public static void createNS(String ns) {
        if (nsExist(ns)) return;

        NamespaceDescriptor desc = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(desc);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 判断命名空间是否存在
     *
     * @param ns
     * @return 存在返回true, 不存在返回false
     */
    private static boolean nsExist(String ns) {
        try {
            admin.getNamespaceDescriptor(ns);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void createTable(String tableName) {
        byte[] cf = Bytes.toBytes(PropertyUtil.getProperty("hbase.cf"));

        try {
            if (admin.tableExists(TableName.valueOf(tableName))) return;

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            desc.addCoprocessor("com.ng.dataconsumer.coprocessor.CallCoprocessor");
            desc.addFamily(new HColumnDescriptor(cf));
            admin.createTable(desc, getSplitKeys());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 计算Hbase表单的分区
     * <p>
     * 0001|
     * 0001|    0002|
     * 0002|    0003|
     * 0003|
     * <p>
     * 0001_xxxx
     *
     * @return
     */
    private static byte[][] getSplitKeys() {
        int regionNum = Integer.parseInt(PropertyUtil.getProperty("hbase.regions"));
        byte[][] keys = new byte[regionNum][];
        for (int i = 1; i <= regionNum; i++) {
            keys[i - 1] = Bytes.toBytes("000" + i + "|");
        }
        return keys;
    }

    /**
     * 根据表名获取到指定的table
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) {
        try {
            return conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据查询的电话号码和年月计算这行记录的分区
     *
     * @param phone
     * @param year
     * @param month
     * @return
     */
    public static String getRegion(String phone, String year, String month) {
        int region = Math.abs((phone + year + "-" + month).hashCode()) % 6 + 1; // 1 - 6
        return decimalFormat.format(region);
    }
}
