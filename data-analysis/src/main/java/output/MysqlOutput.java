package output;

import bean.CommonDimension;
import bean.CountDurationDimension;
import convert.DimensionConvert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.ng.dataconsumer.util.JDBCUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 因为是输出到 Mysql, 所以需要自定义 OutputFormat
 */
public class MysqlOutput extends OutputFormat<CommonDimension, CountDurationDimension> {
    private FileOutputCommitter committer = null;

    /**
     * 获取一个专门用来向外写数据的Writer
     * @param taskAttemptContext
     * @return
     */
    public RecordWriter<CommonDimension, CountDurationDimension> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new MySqlRecordWriter();
    }

    /**
     * 对输出目的地的检测
     * @param jobContext
     */
    public void checkOutputSpecs(JobContext jobContext) {

    }

    /**
     *
     * @param taskAttemptContext
     * @return
     * @throws IOException
     */
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException {
        if (committer == null) {
           Path output =  getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output, taskAttemptContext);
        }
        return null;
    }

    public static Path getOutputPath(JobContext jobContext) {
        String name = jobContext.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    private class MySqlRecordWriter extends RecordWriter<CommonDimension, CountDurationDimension> {

        private Connection conn = JDBCUtil.getInstance();
        private PreparedStatement ps;
        private String sql ;
        private int batchCount = 200;
        private int count = 0;

        public MySqlRecordWriter(){
            try {
                //先往tb_call表插入数据
                sql = "insert into tb_call values(?,?,?,?,?) on DUPLICATE key" +
                        "UPDATE call_count_sum=?, call_duration_sum=?";
                conn.setAutoCommit(false);
                ps = conn.prepareStatement(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        /**
         * 真正核心写出方法
         * 在这个方法内完成向Mysql数据库的写出
         * 使用jdbc向外写
         * <p>
         * 需要些到 3个表中:
         * 联系人:
         * 碰到一个联系人, 写到联系人
         * @param key
         * @param value
         */
        public void write(CommonDimension key, CountDurationDimension value) {

            //1. 拿到联系人的id和日期的id
            int contactId = DimensionConvert.getDimensionId(key.getContactDimension());
            int dateId = DimensionConvert.getDimensionId(key.getDateDimension());

            //2. 向主表中写数据
            try {
                ps.setString(1, dateId + "_" + contactId);
                ps.setInt(2, dateId);
                ps.setInt(3, contactId);
                ps.setInt(4, value.getCountSum());
                ps.setInt(5, value.getDurationSum());
                ps.setInt(6, value.getCountSum());
                ps.setInt(7, value.getDurationSum());
                count++;
                if (count >= batchCount) {
                    ps.executeBatch();
                    conn.commit();
                    count = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        /**
         * 一些关闭资源的操作
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext context)  {
            try {
                ps.executeBatch();
                conn.commit();
                JDBCUtil.close(null, ps, conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
}
