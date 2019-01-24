package mapreduce;

import bean.CommonDimension;
import bean.ContactDimension;
import bean.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CallMapper extends TableMapper<CommonDimension, Text> {

    private Map<String, String> contacts = new HashMap<String, String>();
    private CommonDimension commonDimension = new CommonDimension();
    private ContactDimension contactDimension = commonDimension.getContactDimension();
    private DateDimension dateDimension = commonDimension.getDateDimension();
    private Text valueOUt = new Text();

    @Override
    protected void setup(Context context) {
        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //写数据需要设计到3张表
        //把reduce 需要的数据封装好，然后传递给reduce ， 让reduce最终写到mysql

        //1. 先拿到rowKey  0001_110_2018-01-01 11:11:11_120_0120_0
//        String rowKey = Bytes.toString(key.get());
        String rowKey = Bytes.toString(value.getRow());
        String[] split = rowKey.split("_");
        String duration = split[4];
        String call = split[1];
        String startTime = split[2];

        String year = startTime.substring(0, 4);
        String month = startTime.substring(5, 7);
        String day = startTime.substring(8, 10);

        //封装value
        valueOUt.set(duration);

        //2. 封装数据
        //2.1 封装联系人
        contactDimension.setTelephone(call);
        contactDimension.setName(contacts.get(call));

        //2.2 封装时间

        //日维度 2018 1 1
        dateDimension.setYear(Integer.parseInt(year));
        dateDimension.setMonth(Integer.parseInt(month));
        dateDimension.setDay(Integer.parseInt(day));
        context.write(commonDimension, valueOUt);

        //月维度 2018 1 -1
        dateDimension.setDay(-1);
        context.write(commonDimension, valueOUt);

        //年 2018 -1 -1
        dateDimension.setMonth(-1);
        context.write(commonDimension, valueOUt);
        // commonDimension(100: 2018,1,1  100:2018,1, -1,  100: 2018, -1,-1 )  v(0220)
    }

}


