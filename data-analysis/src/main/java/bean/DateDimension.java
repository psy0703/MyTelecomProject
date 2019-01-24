package bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 日期维度的bean的封装
 */
public class DateDimension implements IDimension{

    private int year;
    private int month;
    private int day;

    public DateDimension() {
    }

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

    @Override
    public String toString() {
        return year + "_" + month + "_" +  day;
    }

    public int compareTo(IDimension o) {
        DateDimension dateDimension = (DateDimension) o;
        int result = this.year - dateDimension.year;
        if (result == 0){
            result = this.month - dateDimension.month;
            if (result == 0){
                result = this.day - dateDimension.day;
            }
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(month);
        out.writeInt(day);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
    }
}
