package lab3_extra;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyWritable implements WritableComparable<KeyWritable> {

    private Text stationId;
    private IntWritable temperature;

    public KeyWritable(){
        this.stationId = new Text();
        this.temperature = new IntWritable();
    }

    public KeyWritable(String stationId, int temparature){
        this.stationId = new Text(stationId);
        this.temperature = new IntWritable(temparature);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        stationId.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(KeyWritable o) {
        if (o == null){
            return -1;
        }
        if (this.stationId.compareTo(o.stationId) == 0){
            return o.temperature.get() - this.temperature.get();
        }else{
            return this.stationId.compareTo(o.stationId);
        }
    }

    @Override
    public String toString() {
        return this.stationId + "   " + this.temperature;
    }

}
