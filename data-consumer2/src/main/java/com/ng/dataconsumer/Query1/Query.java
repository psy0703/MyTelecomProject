package com.ng.dataconsumer.Query1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import com.ng.dataconsumer.util.HBaseFilterUtil;
import com.ng.dataconsumer.util.HBaseUtil;
import com.ng.dataconsumer.util.PropertyUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Query {
    private static String tableName = PropertyUtil.getProperty("hbase.table.name");
    private static String cf = PropertyUtil.getProperty("hbase.cf");
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws IOException {
//        System.out.println(formatter.format(new Date()));
        /*
         查询某个人某天的
         某月
         某年
         的所有通话记录(包括主叫和别叫)

         0001_110_2018-01-01 02:20:20_120_0120_0

         start: 0001_110_2018-01-01
         end:   0001_110_2018-01-01
         */
        queryOneDay("19877232369", 2018, 8, 6);
//        queryOneMonth("19877232369", 2018, 12);

//        queryOneYear("19877232369", 2018);

    }

    public static void queryOneYear(String phone, int year) throws IOException {
        query(phone, year, -1, -1);
    }

    public static void queryOneDay(String phone, int year, int month, int day) throws IOException {
        query(phone, year, month, day);
    }

    public static void queryOneMonth(String phone, int year, int month) throws IOException {
        query(phone, year, month, -1);
    }

    // 2018, 1, 2
    public static void query(String phone, int year, int month, int day) throws IOException {
        // call1 和 call2 是或的关系   然后在和时间与
        Table table = HBaseUtil.getTable(tableName);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(cf));
        Filter f1 = HBaseFilterUtil.eqFilter(cf, "call1", Bytes.toBytes(phone));
        Filter f2 = HBaseFilterUtil.eqFilter(cf, "call2", Bytes.toBytes(phone));

        Filter f12 = HBaseFilterUtil.orFilter(f1, f2);

        Calendar calendar = Calendar.getInstance();
        Filter f3 = null;

        /*if (day >= 1) {
            calendar.set(year, month - 1, day);
            f3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()));
        }else if(month != -1 && day == -1){
            calendar.set(year, month - 1, 1);
            f3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()).substring(0, 7));
        }else if(month == -1 && day == -1){
            calendar.set(year, 0, 1);
            f3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()).substring(0, 4));
        }*/

        if (month == -1 && day == -1) {
            calendar.set(year, 0, 1);
            f3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()).substring(0, 4));
        } else if (day == -1) {
            calendar.set(year, month - 1, 1);
            f3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()).substring(0, 7));
        } else {
            calendar.set(year, month - 1, day);
            f3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()));
        }

        Filter f123 = HBaseFilterUtil.andFilter(f12, f3);
        scan.setFilter(f123);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String s = Bytes.toString(result.getRow());
            System.out.println(s);
        }
    }
}
