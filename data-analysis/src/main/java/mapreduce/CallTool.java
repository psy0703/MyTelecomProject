package mapreduce;

import bean.CommonDimension;
import bean.CountDurationDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import output.MysqlOutput;

public class CallTool implements Tool {

    private Configuration conf;


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


    public void setConf(Configuration conf) {
        this.conf = conf;
    }


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
