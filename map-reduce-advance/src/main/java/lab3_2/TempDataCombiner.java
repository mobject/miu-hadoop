package lab3_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempDataCombiner extends Reducer<Text, Pair, Text, Pair> {
    @Override
    protected void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for(Pair temp : values){
            sum += temp.getSum().get();
            count += temp.getCount().get();
        }
        System.out.println("-----"+sum);
        System.out.println("+++++"+count);
        context.write(key, new Pair(sum, count));
    }
}
