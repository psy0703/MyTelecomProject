package com.ng.dataconsumer.coprocessor;

import com.ng.dataconsumer.Dao.HBaseDao;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CallCoprocessor extends BaseRegionObserver {

    private HBaseDao dao = new HBaseDao();
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 覆写 postPut 方法, put 方法执行完毕之后执行此方法
     * <p>
     * 当插入一条主叫记录之后, 再这里再插入一条被叫记录
     * <p>
     * 由于插入被叫信息也是会执行 put 方法, 所以会再次出发这个方法, 一定要防止死循环.
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {
        //再插入一条被叫记录
        // 插入主叫数据时的行 0001_110_2018-01-01 01:01:01_120_0110_0
        String rowKey = Bytes.toString(put.getRow());

        // 0002_120_2018-01-01 01:01:01_110_0110_1
        // 组装成这样的字符串 120,110,34093240,110,1
        String[] split = rowKey.split("_");
        if ("1".equals(split[5])) {
            return;// 如果插入的是被叫信息, 则直接返回不做任何操作.
        }
        try {
            String r = split[3] + "," + split[1] + "," + formatter.parse(split[2]).getTime() + "," + split[4] + "," + 1;
            dao.put(r);
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

    }
}
