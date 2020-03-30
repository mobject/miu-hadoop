import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class AvroGenericStationTempYear extends Configured implements Tool
{

	private static Schema SCHEMA;

	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, FloatWritable>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);
		AvroKey<GenericRecord> avroKey = new AvroKey<>(record);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				record.put("StationId", utils.getStationId());
				record.put("Year", utils.getYearInt());
				
				//populate the avroKey with record
				
				context.write(avroKey, new FloatWritable(utils.getAirTemperature()));
			}
		}
	}

	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, FloatWritable, AvroKey<GenericRecord>, NullWritable>
	{

		private GenericRecord record = new GenericData.Record(SCHEMA);
		AvroKey<GenericRecord> avroKey = new AvroKey<>(record);

		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException
		{
			FloatWritable max = null;
			System.out.println(key.datum());
			System.out.println(max);
			for (FloatWritable value : values) {
				if (max == null || value.get() > max.get())
					max = new FloatWritable((value.get()));
			}
			GenericRecord datum = key.datum();
			datum.put("MaxTemp", max.get());
			avroKey.datum(datum);
			context.write(avroKey,NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroGenericStationTempYear.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);
		job.setMapOutputValueClass(FloatWritable.class);

		AvroJob.setMapOutputKeySchema(job, SCHEMA);
		AvroJob.setOutputKeySchema(job, SCHEMA);

		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner.run(conf, new AvroGenericStationTempYear(), args);		
		System.exit(res);
	}
}