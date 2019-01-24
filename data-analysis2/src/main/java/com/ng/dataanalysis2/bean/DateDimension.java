package com.ng.dataanalysis2.bean;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateDimension implements IDimension {
    private int year;
    private int month;
    private int day;


    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }


    public int compareTo(IDimension o) {
        DateDimension other = (DateDimension) o;

        int result = this.year - other.year;
        if (result == 0) {
            result = this.month - other.month;
            if (result == 0) {
                result = this.day - other.day;
            }
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
    }

    @Override
    public String toString() {
        return year + "_" + month + "_" + day;
    }
}

