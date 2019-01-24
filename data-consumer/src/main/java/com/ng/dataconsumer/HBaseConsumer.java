package com.ng.dataconsumer;

import com.ng.dataconsumer.Dao.HBaseDao;
import com.ng.dataconsumer.Utils.PropertyUtil;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Collections;

public class HBaseConsumer {

    /**
     * 从Kafka读取数据
     * @param callBack
     */
    public void readDataFromKafka(CallBack callBack){
        KafkaConsumer<String , String> consumer = new KafkaConsumer<>(PropertyUtil.properties);
        //订阅主体
        consumer.subscribe(Collections.singletonList("calllog"));
        //拉取数据
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(6000);
            for (ConsumerRecord<String, String> record : records) {
                //从Kafka读取到数据
                if (callBack != null){
                    callBack.call(record.value());
                }
            }
        }
    }

    public interface  CallBack{
        void call(String data);
    }

    public static void main(String[] args) {
        //1.从kafka读数据
        HBaseConsumer hBaseConsumer = new HBaseConsumer();
       /* hBaseConsumer.readDataFromKafka(new CallBack() {
            @Override
            public void call(String data) {
                HBaseDao com.ng.dataconsumer.dao = new HBaseDao();
                // 对读到数据做处理:  //2. 读取到的数据写入到HBase
                com.ng.dataconsumer.dao.put(data);
            }
        });*/
        HBaseDao dao = new HBaseDao();
        hBaseConsumer.readDataFromKafka(data -> dao.put(data));
    }
}
