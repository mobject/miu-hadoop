package lab3_5;

import org.apache.hadoop.mapreduce.Partitioner;

public class JobPartitioner extends Partitioner<YearWritable, Pair> {

    @Override
    public int getPartition(YearWritable yearWritable, Pair pair, int numPartitions) {
        YearWritable year1930 = new YearWritable("1930");
        if (year1930.compareTo(yearWritable) < 0){
            return 0;
        }
        return 1;
    }
}
