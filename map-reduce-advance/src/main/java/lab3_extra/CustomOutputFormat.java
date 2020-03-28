package lab3_extra;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CustomOutputFormat extends TextOutputFormat<KeyWritable, Text> {

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context,
                                   String extension) throws IOException {
        Path baseOutputPath = FileOutputFormat.getOutputPath(context);

        return new Path(baseOutputPath, "StationTempRecord");
    }
}
