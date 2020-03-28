package lab3_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TempDataMapper extends Mapper<LongWritable, Text, Text, Pair> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String dateStr = value.toString().substring(15, 19);

        String temperatureStr = value.toString().substring(87, 92);
        double temperature = Double.parseDouble(temperatureStr) / 10;
        context.write(new Text(dateStr), new Pair(temperature,1.0));
    }
}
