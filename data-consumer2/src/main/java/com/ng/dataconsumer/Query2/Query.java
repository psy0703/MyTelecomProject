package com.ng.dataconsumer.Query2;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.ng.dataconsumer.util.HBaseUtil;
import com.ng.dataconsumer.util.PropertyUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class Query {
    private static String tableName = PropertyUtil.getProperty("hbase.table.name");
    private static String cf = PropertyUtil.getProperty("hbase.cf");
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws IOException {
//        queryOneDay("19877232369", 2018, 8, 6);
        queryOneMonth("19879419704", 2018, 4);
//        queryOneYear("19877232369", 2018);

    }

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


    // 0001_110_2018-01-01  start  0001_110_2018-01-02_xxxxx
    // 0001_110_2018-01-01|  end
    public static void query(String phone, int year, int month, int day) throws IOException {
        DecimalFormat f = new DecimalFormat("00");

        Table table = HBaseUtil.getTable(tableName);
        Scan scan = new Scan();
        byte[] start = null;
        byte[] end = null;
        if (day != -1) {
            start = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", f.format(month))
                    + "_" + phone
                    + "_" + year + "-" + f.format(month) + "-" + f.format(day));

            end = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", f.format(month))
                    + "_" + phone
                    + "_" + year + "-" + f.format(month) + "-" + f.format(day + 1));
        } else if (month != -1 && day == -1) {
            start = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", f.format(month))
                    + "_" + phone
                    + "_" + year + "-" + f.format(month));  // 0001_110_2018-01

            end = Bytes.toBytes(HBaseUtil.getRegion(phone, year + "", f.format(month))
                    + "_" + phone
                    + "_" + year + "-" + f.format(month + 1)); // 0001_110_2018_02
        }


        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
        }
    }
}
