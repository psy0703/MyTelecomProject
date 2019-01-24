package com.ng.dataconsumer.coprocessor;
import com.ng.dataconsumer.dao.HBaseDao;
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

/**
 *
 */
public class CallCoprocessor extends BaseRegionObserver {
    private HBaseDao dao = new HBaseDao();
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 当插入一条记录之后, 会回调这个方法
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {
        // 再插入一条被叫记录
        // 0001_110_2018-01-01 01:01:01_120_0110_0
        String rowKey = Bytes.toString(put.getRow());

        // 0002_210_2018-01-01 01:01:01_110_0110_1
        // 120,110,34093240,110,1

        String[] split = rowKey.split("_");
        if (split[5].equals("1")) return;
        try {
            String r = split[3] + "," + split[1] + "," + formatter.parse(split[2]).getTime() + "," + split[4] + "," + 1;
            dao.put(r);
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
    }
}

