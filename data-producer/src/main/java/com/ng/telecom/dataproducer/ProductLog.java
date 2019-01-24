package com.ng.telecom.dataproducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * 生成通话日志记录
 */
public class ProductLog {

    public static void writeLog(){
        String path = "/opt/module/datas/telecom/calls.csv";
        try( PrintWriter writer = new PrintWriter(new File(path)) ){
            while (true){
                CallRecord record = new CallRecord();
                writer.print(record.toString() + "\n");
                writer.flush();
                Thread.sleep(500);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        writeLog();
    }
}
