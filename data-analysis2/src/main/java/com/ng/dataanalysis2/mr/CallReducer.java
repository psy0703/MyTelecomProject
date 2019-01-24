package com.ng.dataanalysis2.mr;
import com.ng.dataanalysis2.bean.CommonDimension;
import com.ng.dataanalysis2.bean.CountDurationDimension;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// 2018,1,1
public class CallReducer extends Reducer<CommonDimension, Text, CommonDimension, CountDurationDimension> {
    private CountDurationDimension countDurationDimension = new CountDurationDimension();

    @Override
    protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int countSum = 0;
        int durationSum = 0;

        // 统计出通话时长和通话次数
        for (Text value : values) {
            countSum++;
            durationSum += Integer.parseInt(value.toString());
        }
        countDurationDimension.setCountSum(countSum);
        countDurationDimension.setDurationSum(durationSum);
        // 写出去了
        context.write(key, countDurationDimension);
    }
}
