package lab3_4;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class TemperatureTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "AvgTemperature");
        job.setMapperClass(TempDataMapper.class);
        job.setReducerClass(TempDataReducer.class);
        job.setOutputKeyClass(YearWritable.class);
        job.setOutputValueClass(Pair.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);

        FileSystem fileSystem = outputPath.getFileSystem(job.getConfiguration());
        boolean exists = fileSystem.exists(outputPath);
        if (exists){
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
