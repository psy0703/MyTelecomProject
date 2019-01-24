package com.ng.dataconsumer.Dao;

import com.ng.dataconsumer.Utils.HBaseUtil;
import com.ng.dataconsumer.Utils.PropertyUtil;
import com.sun.jdi.IntegerType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 该类主要用于执行具体的保存数据的操作，rowkey 的生成规则等等。
 */
public class HBaseDao {

    private final Table table;
    private final String CF;
    private final int REGION_NUM;
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    DecimalFormat decimalFormat = new DecimalFormat("0000");

    public HBaseDao(){
        Properties properties = PropertyUtil.properties;
        CF = properties.getProperty("hbase.cf");
        REGION_NUM = Integer.parseInt(properties.getProperty("hbase.regions"));
        //1.创建命名空间
        HBaseUtil.createNameSpace(properties.getProperty("hbase.namespace"));
        //2.创建表
        HBaseUtil.createTable(properties.getProperty("hbase.table.name"));

        //3.获取要操作的表
        table = HBaseUtil.getTable(properties.getProperty("hbase.table.name"));
    }

    /**
     * 将读取到的数据写入HBASE中
     * call1,call2,starttime,duration,flag
     * 19877232369,18674257265,1516920728046,6123,0
     * @param data
     */
    public void put(String data){
        System.out.println(data);
        String[] split = data.split(",");
        try {
            //2.写数据
            Put put = new Put(getRowKey(split));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("call1"), Bytes.toBytes(split[0]));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("call2"), Bytes.toBytes(split[1]));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("startTime"), Bytes.toBytes(formatter.format(new Date(Long.parseLong(split[2])))));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("duration"), Bytes.toBytes(split[3]));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("flag"), Bytes.toBytes(split[4]));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取每条数据的行键
     * call1,call2,starttime,duration,flag
     * <p>
     * xxxx_
     * <p>
     * 0001_110_2018-01-02 00:12:22_120_0011_0
     *
     * @return
     */
    private byte[] getRowKey(String[] split) {
      String region = getRegion(split);
        //19877232369,18674257265,1516920728046,0023,0
        return Bytes.toBytes(region + "_"
        + split[0] + "_"
        + formatter.format(new Date(Long.parseLong(split[2]))) + "_"
        + split[1] + "_"
        + decimalFormat.format(Integer.parseInt(split[3])) + "_"
        + split[4]);

    }

    /**
     * 返回这个行数据应该进的区
     * 0001
     * 0002
     * 0003
     * ...
     * @param split
     */
    private String getRegion(String[] split) {
        //只跟call1 和年月有关
        String call1 = split[0];
        String time = split[2];
        // 2018-02
        String yearMonth = formatter.format(new Date(Long.parseLong(time))).substring(0, 7);
        //1 - 6
        int region = Math.abs((call1 + yearMonth).hashCode()) % REGION_NUM + 1 ;
        return decimalFormat.format(region);
    }
    public static void main(String[] args) {
        HBaseDao hBaseDao = new HBaseDao();
    }
}
