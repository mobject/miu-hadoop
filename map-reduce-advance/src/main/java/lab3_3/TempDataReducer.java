package lab3_3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempDataReducer extends Reducer<Text, Pair, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for(Pair temp : values){
            sum += temp.getSum().get();
            count += temp.getCount().get();
        }
        double avg = (sum/count)*10;
        context.write(key, new DoubleWritable(avg));
    }
}
