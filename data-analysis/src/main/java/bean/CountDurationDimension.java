package bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * producer 向外写的数据的值类型的封装.
 * 包括通话次数和通话时长
 */
public class CountDurationDimension implements Writable {
    private int countSum;
    private int durationSum;

    public CountDurationDimension() {
    }

    public int getCountSum() {
        return countSum;
    }

    public void setCountSum(int countSum) {
        this.countSum = countSum;
    }

    public int getDurationSum() {
        return durationSum;
    }

    public void setDurationSum(int durationSum) {
        this.durationSum = durationSum;
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(countSum);
        out.writeInt(durationSum);
    }

    public void readFields(DataInput in) throws IOException {
        this.countSum = in.readInt();
        this.durationSum = in.readInt();
    }
}
