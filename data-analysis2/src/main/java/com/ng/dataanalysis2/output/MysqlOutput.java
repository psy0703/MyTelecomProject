package com.ng.dataanalysis2.output;
import com.ng.dataanalysis2.bean.CommonDimension;
import com.ng.dataanalysis2.bean.CountDurationDimension;
import com.ng.dataanalysis2.convert.DimensionConvert;
import com.ng.dataanalysis2.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutput extends OutputFormat<CommonDimension, CountDurationDimension> {
    private FileOutputCommitter committer = null;

    /**
     * 获取一个专门用来向外写数据的 Writer
     *
     * @param context
     * @return
     */
    @Override
    public RecordWriter<CommonDimension, CountDurationDimension> getRecordWriter(TaskAttemptContext context) {
        return new MySqlRecordWriter();
    }

    /**
     * 对输出目的地的检测
     *
     * @param context
     */
    @Override
    public void checkOutputSpecs(JobContext context) {

    }

    /**
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    private static class MySqlRecordWriter extends RecordWriter<CommonDimension, CountDurationDimension> {
        private Connection conn = JDBCUtil.getInstance();
        private PreparedStatement ps;
        private String sql;
        private int batchCount = 200;
        private int count = 0;

        public MySqlRecordWriter(){
            try {
                // 先tb_call表插入数据
                sql = "insert into tb_call values(?, ?, ?, ?, ?) " +
                        "on DUPLICATE key UPDATE call_count_sum=?, call_duration_sum=?";
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
         *
         * @param key
         * @param value
         */
        @Override
        public void write(CommonDimension key, CountDurationDimension value) {
            // 1. 拿到联系人的id和日期的id
            int contactId = DimensionConvert.getDimensionId(key.getContactDimension());
            int dateId = DimensionConvert.getDimensionId(key.getDateDimension());

            // 2. 向 主表中写入数据
            try {
                ps.setString(1, dateId + "_" + contactId);
                ps.setInt(2, dateId);
                ps.setInt(3, contactId);
                ps.setInt(4, value.getCountSum());
                ps.setInt(5, value.getDurationSum());
                ps.setInt(6, value.getCountSum());
                ps.setInt(7, value.getDurationSum());
                ps.addBatch();
                count++;
                if(count >= batchCount){
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
         *
         * @param context
         */
        @Override
        public void close(TaskAttemptContext context) {
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

