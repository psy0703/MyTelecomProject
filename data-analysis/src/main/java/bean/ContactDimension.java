package bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 联系人维度bean封装
 */
public class ContactDimension implements IDimension{

    private String telephone;
    private String name;

    public ContactDimension() {

    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return  telephone + "_" + name;
    }

    public int compareTo(IDimension o) {
        ContactDimension contactDimension = (ContactDimension) o;
        return this.telephone.compareTo(contactDimension.telephone);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(telephone);
        out.writeUTF(name);
    }

    public void readFields(DataInput in) throws IOException {
        this.telephone = in.readUTF();
        this.name = in.readUTF();
    }
}
