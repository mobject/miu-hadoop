package lab3_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

public class TempDataMapper extends Mapper<LongWritable, Text, Text, Pair> {

    private Hashtable<String, Pair> hashtable = new Hashtable<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String dateStr = value.toString().substring(15, 19);
        Pair pair = hashtable.get(dateStr);
        if (pair == null){
            pair = new Pair();
        }
        String temperatureStr = value.toString().substring(87, 92);
        double temperature = Double.parseDouble(temperatureStr) / 10;
        pair.increaseCount();
        pair.addSum(temperature);
        hashtable.put(dateStr, pair);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Pair> entry : hashtable.entrySet()){
            context.write(new Text(entry.getKey()), hashtable.get(entry.getKey()));
        }
    }
}
