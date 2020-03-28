package lab3_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements Writable {

    private DoubleWritable sum;
    private DoubleWritable count;

    public Pair(){
        this.sum = new DoubleWritable();
        this.count = new DoubleWritable();
    }

    public Pair(double sum, double count){
        this.sum = new DoubleWritable(sum);
        this.count = new DoubleWritable(count);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.sum.write(out);
        this.count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sum.readFields(in);
        this.count.readFields(in);
    }

    public DoubleWritable getSum() {
        return sum;
    }

    public DoubleWritable getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }
}
