package bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Mapper 向 Reducer 传递的数据的封装
 */
public class CommonDimension implements WritableComparable<CommonDimension> {
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    public CommonDimension() {
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public int compareTo(CommonDimension o) {
        int result = this.contactDimension.compareTo(o.contactDimension);
        if (result == 0) {
            this.dateDimension.compareTo(o.dateDimension);
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        this.contactDimension.write(out);
        this.dateDimension.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.contactDimension.readFields(in);
        this.dateDimension.readFields(in);
    }

    @Override
    public String toString() {
        return this.contactDimension + "," + this.dateDimension;
    }
}
