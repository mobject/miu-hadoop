package lab3_4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempDataReducer extends Reducer<YearWritable, Pair, Text, DoubleWritable> {
    @Override
    protected void reduce(YearWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for (Pair temp : values) {
            sum += temp.getSum().get();
            count += temp.getCount().get();
        }
        System.out.println("-----" + sum);
        System.out.println("+++++" + count);
        double avg = (sum / count) * 10;
        context.write(key.getYear(), new DoubleWritable(avg));
    }
}
