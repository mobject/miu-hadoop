package lab3_extra;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempDataReducer extends Reducer<KeyWritable, Text, Text, Text> {
    @Override
    protected void reduce(KeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value: values){
            context.write(new Text(key.toString()), value);
        }
    }
}
