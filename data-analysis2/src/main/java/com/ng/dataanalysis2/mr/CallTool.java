package com.ng.dataanalysis2.mr;
import com.ng.dataanalysis2.bean.CommonDimension;
import com.ng.dataanalysis2.bean.CountDurationDimension;
import com.ng.dataanalysis2.output.MysqlOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CallTool implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        //
        Job job = Job.getInstance(getConf());
        job.setJarByClass(CallTool.class);

        // 设置Mapper
        TableMapReduceUtil.initTableMapperJob(
                "ns_telecom:calllog",
                new Scan(),
                CallMapper.class,
                CommonDimension.class,
                Text.class,
                job);
        job.setReducerClass(CallReducer.class);
        job.setOutputKeyClass(CommonDimension.class);
        job.setOutputValueClass(CountDurationDimension.class);
        // 设置 OutputFormat 使用我们自定义的
        job.setOutputFormatClass(MysqlOutput.class);
        job.addFileToClassPath(new Path("hdfs://psy831:9000/libs/mysql-connector-java-5.1.27-bin.jar"));
        boolean flag = job.waitForCompletion(true);
        return flag ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new CallTool(), args);
        if(code == 0){
            System.out.println("正常结束");
        }else{
            System.out.println("异常结束");
        }
    }
}

