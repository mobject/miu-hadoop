package lab3_4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearWritable implements WritableComparable<YearWritable> {

    private Text year;

    public YearWritable(){
        this.year = new Text();
    }

    public YearWritable(String year){
        this.year = new Text(year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
    }

    @Override
    public int compareTo(YearWritable o) {
        if (o == null || o.year == null || this.year == null){
            return 1;
        }
        Integer year1 = Integer.parseInt(o.year.toString());
        Integer year2 = Integer.parseInt(this.year.toString());
        return year1 - year2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearWritable that = (YearWritable) o;
        return Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year);
    }

    @Override
    public String toString() {
        return "YearWritable{" +
                "year=" + year +
                '}';
    }

    public Text getYear() {
        return year;
    }
}
