package lab3_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TempDataMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String dateStr = value.toString().substring(15, 19);
        String temperatureStr = value.toString().substring(87, 92);
        double temperature = Double.parseDouble(temperatureStr) / 10;
        DoubleWritable doubleWritable = new DoubleWritable(temperature);
        context.write(new Text(dateStr), doubleWritable);
    }
}
