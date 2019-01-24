package com.ng.dataconsumer;

import com.ng.dataconsumer.dao.HBaseDao;
import com.ng.dataconsumer.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class HBaseConsumer2 {
    public static void main(String[] args) {
        //1. 从kafka读数据
        HBaseConsumer2 hBaseConsumer = new HBaseConsumer2();
        /*hBaseConsumer.readDataFromKafka(new CallBack(){
            @Override
            public void call(String data) {
                // 对读到数据做处理:  //2. 读取到的数据写入到HBase
                dao.put(data);
            }
        });*/
        /*HBaseDao dao = new HBaseDao();
        hBaseConsumer.readDataFromKafka(data ->{
            // 对读到数据做处理:  //2. 读取到的数据写入到HBase
            dao.put(data);
        });*/

        HBaseDao dao = new HBaseDao();
        hBaseConsumer.readDataFromKafka(data -> {
            dao.put(data);

            // call1,call2,startTime,duration,0
            // 变成
            //call2,call1,startTime,duration,1
            System.out.println(data);
            String[] split = data.split(",");
            String temp = split[0];
            split[0] = split[1];
            split[1] = temp;
            split[4] = "1";
            dao.put(split[0] + "," + split[1] + "," + split[2] + "," + split[3] + "," + split[4]);
            System.out.println("主叫:" + data);
            System.out.println("被叫:" + split[0] + "," + split[1] + "," + split[2] + "," + split[3] + "," + split[4]);
        });
    }

    /**
     * 从Kafka读取数据
     */
    public void readDataFromKafka(CallBack callBack) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(PropertyUtil.properties);
        //订阅主体
        consumer.subscribe(Collections.singletonList("calllog"));
        //拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(6000);
            for (ConsumerRecord<String, String> record : records) {
                // 从kafka读取到了数据
                if (callBack != null) {
                    callBack.call(record.value());
                }

            }
        }
    }

    /**
     * 定义一个回调接口
     */
    public interface CallBack {
        void call(String data);
    }
}
