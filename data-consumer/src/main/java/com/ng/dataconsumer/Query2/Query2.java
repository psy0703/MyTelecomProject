package com.ng.dataconsumer.Query2;

import com.ng.dataconsumer.Utils.HBaseUtil;
import com.ng.dataconsumer.Utils.PropertyUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class Query2 {
    private static String tableName = PropertyUtil.getProperty("hbase.table.name");
    private static String cf = PropertyUtil.getProperty("hbase.cf");
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");


    public static void queryOneYear(String phone, int year) throws IOException {
        for (int i = 1; i <= 12; i++) {
            query(phone, year, i, -1);
        }
    }

    public static void queryOneMonth(String phone, int year, int month) throws IOException {
        query(phone, year, month, -1);
    }

    public static void queryOneDay(String phone, int year, int month, int day) throws IOException {
        query(phone, year, month, day);
    }

    public static void query(String phone, int year, int month ,int day) throws IOException {
        DecimalFormat format = new DecimalFormat("00");

        Table table = HBaseUtil.getTable(tableName);
        Scan scan = new Scan();
        byte[] start = null;
        byte[] end = null;

        // 0001_110_2018-01-01  start  0001_110_2018-01-02_xxxxx
        // 0001_110_2018-01-01|  end
        if (day != -1){
            start = Bytes.toBytes(HBaseUtil.getRegion(phone,
                    year + "", format.format(month))
                    + "_" + phone
                    + "_" + year + "-" + format.format(month)
                    + "-" + format.format(day));

            end = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", format.format(month))
                    + "_" + phone
                    + "_" + year + "-" + format.format(month)
                    + "-" + format.format(day + 1));
        } else if (month != -1 && day == -1){
            start = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", format.format(month))
                    + "_" + phone
                    + "_" + year + "-" + format.format(month));  // 0001_110_2018-01

            end = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", format.format(month))
                    + "_" + phone
                    + "_" + year + "-" + format.format(month + 1)); // 0001_110_2018_02
        }

        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            String row = Bytes.toString(result.getRow());
            System.out.println(row);
        }
    }

}

