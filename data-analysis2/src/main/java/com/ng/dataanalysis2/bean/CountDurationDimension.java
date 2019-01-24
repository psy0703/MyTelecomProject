package com.ng.dataanalysis2.bean;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountDurationDimension implements Writable {
    private  int countSum;
    private int durationSum;

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
        out.write(countSum);
        out.write(durationSum);
    }

    public void readFields(DataInput in) throws IOException {
        this.countSum = in.readInt();
        this.durationSum = in.readInt();
    }
}

