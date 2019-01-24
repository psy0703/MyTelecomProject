package com.ng.dataconsumer.dao;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.ng.dataconsumer.util.HBaseUtil;
import com.ng.dataconsumer.util.PropertyUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class HBaseDao {
    private final Table table;
    private final String CF;
    private final int REGION_NUM;
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    DecimalFormat decimalFormat = new DecimalFormat("0000");

    public HBaseDao() {
        Properties properties = PropertyUtil.properties;
        CF = properties.getProperty("hbase.cf");
        REGION_NUM = Integer.parseInt(properties.getProperty("hbase.regions"));
        // 1.创建命名空间
        HBaseUtil.createNS(properties.getProperty("hbase.namespace"));
        // 2.创建表
        HBaseUtil.createTable(properties.getProperty("hbase.table.name"));

        // 3. 获取要操作表
        table = HBaseUtil.getTable(properties.getProperty("hbase.table.name"));
    }

    /**
     * call1,call2,starttime,duration,flag
     * 19877232369,18674257265,1516920728046,6123,0
     *
     * @param data
     */
    public void put(String data) {
        System.out.println(data);
        String[] split = data.split(",");
        try {
            // 2. 写数据
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
     *
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
     *
     * @param split
     */
    private String getRegion(String[] split) {
        // 只和call1和年月有关
        String call1 = split[0];
        String time = split[2];
        String yearMonth = formatter.format(new Date(Long.parseLong(time))).substring(0, 7);  // 2018-02

        return HBaseUtil.getRegion(
                call1,
                yearMonth.substring(0, 4),
                yearMonth.substring(5, 7));

    }
}
