package lab3_extra;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

public class TempDataMapper extends Mapper<LongWritable, Text, KeyWritable, Text> {

//    private Hashtable<String, Pair> hashtable = new Hashtable<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String usStationIdStr = value.toString().substring(11, 15);
        String wbanStationIdStr = value.toString().substring(3, 11);

        String temperatureStr = value.toString().substring(87, 92);
        int temperature = Integer.parseInt(temperatureStr) / 10;
        String dateStr = value.toString().substring(15, 19);
        context.write(new KeyWritable( wbanStationIdStr+ "-" + usStationIdStr, temperature),new Text(dateStr));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for (Map.Entry<String, Pair> entry : hashtable.entrySet()){
//            context.write(new YearWritable(entry.getKey()), hashtable.get(entry.getKey()));
//        }
    }
}
