package com.ng.dataconsumer.Qeuery1;

import com.ng.dataconsumer.Utils.HBaseFilterUtil;
import com.ng.dataconsumer.Utils.HBaseUtil;
import com.ng.dataconsumer.Utils.PropertyUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Query1 {

    private static String tableName = PropertyUtil.getProperty("hbase.table.name");
    private static String cf = PropertyUtil.getProperty("hbase.cf");
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 查询某一天的通话记录
     * @param phone 查询的电话号码
     * @param year 年
     * @param month 月
     * @param day 日
     * @throws IOException
     */
    public static void queryOneDay(String phone,int year, int month, int day) throws IOException {
        query(phone, year, month, day);
    }

    /**
     * 查询某一月的通话记录
     * @param phone 查询的电话号码
     * @param year 年
     * @param month 月
     * @throws IOException
     */
    public static void queryOneMonth(String phone, int year , int month) throws IOException {
        query(phone, year, month, -1);
    }

    /**
     * 查询某一年的通话记录
     * @param phone 查询的电话号码
     * @param year 年
     * @throws IOException
     */
    public static void queryOneYear(String phone, int year) throws IOException {
        query(phone, year, -1, -1);
    }


    public static void query(String phone, int year, int month, int day) throws IOException {
        //call1 和 call2 是或的关系  然后再和时间与
        Table table = HBaseUtil.getTable(tableName);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(cf));

        Filter filter1 = HBaseFilterUtil.eqFilter(cf, "call1", Bytes.toBytes(phone));//主叫号码与要查询的一致
        Filter filter2 = HBaseFilterUtil.eqFilter(cf, "call2", Bytes.toBytes(phone));//被叫号码与要查询的一致

        Filter filter12 = HBaseFilterUtil.orFilter(filter1, filter2); //call1 和 call2 是或关系

        Calendar calendar = Calendar.getInstance();
        Filter filter3 = null;

        if (month == -1 && day == -1){ //只查询某年
            calendar.set(year, 0, 1);
            filter3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()).substring(0, 4)); //只取年份
        } else if (day == -1){ //查询某年某月
            calendar.set(year, month - 1, 1);
            filter3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()).substring(0,7));
        } else { //查询某年某月某日
            calendar.set(year, month - 1, day);
            filter3 = HBaseFilterUtil.subStringFilter(
                    cf,
                    "startTime",
                    formatter.format(calendar.getTimeInMillis()));
        }

        Filter f123 = HBaseFilterUtil.andFilter(filter12, filter3); //filter12 与 时间是与关系

        scan.setFilter(f123);

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            String row = Bytes.toString(result.getRow());
            System.out.println(row);
        }


    }
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
}
