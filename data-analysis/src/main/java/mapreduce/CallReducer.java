package mapreduce;

import bean.CommonDimension;
import bean.CountDurationDimension;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CallReducer extends Reducer<CommonDimension,Text,CommonDimension, CountDurationDimension> {

    private CountDurationDimension countDurationDimension = new CountDurationDimension();

    @Override
    protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int countSum = 0;
        int durationSum = 0;

        //统计出通话时长和通话次数
        for (Text value : values) {
            countSum++;
            durationSum += Integer.parseInt(value.toString());
        }

        //把两个值封装到对象中
        countDurationDimension.setCountSum(countSum);
        countDurationDimension.setDurationSum(durationSum);

        // 写出数据, 然后让output来处理数据应该如何写到 Mysql 中
        context.write(key, countDurationDimension);
    }
}
