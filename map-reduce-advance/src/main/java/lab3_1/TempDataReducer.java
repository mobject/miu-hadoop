package lab3_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempDataReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for(DoubleWritable temp : values){
            sum += temp.get();
            count += 1;
        }
        System.out.println("-----"+sum);
        System.out.println("+++++"+count);
        double avg = (sum/count)*10;
        context.write(key, new DoubleWritable(avg));
    }
}
