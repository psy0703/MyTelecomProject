package com.ng.dataconsumer;

import com.ng.dataconsumer.dao.HBaseDao;
import com.ng.dataconsumer.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class HBaseConsumer {
    public static void main(String[] args) {
        //1. 从kafka读数据
        HBaseConsumer hBaseConsumer = new HBaseConsumer();
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
        hBaseConsumer.readDataFromKafka(data -> dao.put(data));
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
