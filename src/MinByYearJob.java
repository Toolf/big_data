import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MinByYearJob {

	private static final Text MARKET_KEY1 = new Text("EURUSD");
	private static final Text MARKET_KEY2 = new Text("EURGBP");
	private static final Text MARKET_KEY3 = new Text("EURCHF");

	public static class MapperResult implements Writable {
		private double opening;
		private Text key;

		public MapperResult() {
		}

		public MapperResult(double opening, Text key) {
			this.opening = opening;
			this.key = key;
		}

		public double getOpening() {
			return opening;
		}

		public String getStringKey() {
			return key.toString();
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			dataOutput.writeDouble(opening);
			dataOutput.writeUTF(key.toString());
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			opening = dataInput.readDouble();
			key = new Text(dataInput.readUTF());
		}

	}

	public static class MinByYearMapper extends Mapper<Object, Text, IntWritable, MapperResult> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String csvLine = value.toString();
			String[] csvField = csvLine.split(",");

			String date1 = csvField[0];
			String date2 = csvField[0 + 7];
			String date3 = csvField[0 + 14];
			IntWritable year1 = IntWritable(Integer.parseInt(date.split("\\.")[0]));
			IntWritable year2 = IntWritable(Integer.parseInt(date.split("\\.")[0]));
			IntWritable year3 = IntWritable(Integer.parseInt(date.split("\\.")[0]));
			Double open1 = Double.parseDouble(csvField[2]);
			Double open2 = Double.parseDouble(csvField[2 + 7]);
			Double open3 = Double.parseDouble(csvField[2 + 14]);

			context.write(year1, new MapperResult(open1, MARKET_KEY1));
			context.write(year2, new MapperResult(open2, MARKET_KEY2));
			context.write(year3, new MapperResult(open3, MARKET_KEY2));
		}
	}

	public static class MinByYearCombiner
			extends Reducer<IntWritable, MapperResult, IntWritable, MapperResult> {
		@Override
		public void reduce(IntWritable key, Iterable<MapperResult> values, Context context)
				throws IOException, InterruptedException {
			double min1 = Double.MAX_VALUE;
			double min2 = Double.MAX_VALUE;
			double min3 = Double.MAX_VALUE;

			for (MapperResult mappingResult : values) {
				if (mappingResult.getStringKey().equals(MARKET_KEY1.toString())) {
					min1 = Math.min(min1, mappingResult.getOpening());
				} else if (mappingResult.getStringKey().equals(MARKET_KEY2.toString())) {
					min2 = Math.min(min2, mappingResult.getOpening());
				} else {
					min3 = Math.min(min3, mappingResult.getOpening());
				}
			}

			context.write(key, new MapperResult(min1, MARKET_KEY1));
			context.write(key, new MapperResult(min2, MARKET_KEY2));
			context.write(key, new MapperResult(min3, MARKET_KEY3));
		}
	}

	public static class MinByYearReducer extends Reducer<IntWritable, MapperResult, Text, DoubleWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<MapperResult> values, Context context)
				throws IOException, InterruptedException {
			double min1 = Double.MAX_VALUE;
			double min2 = Double.MAX_VALUE;
			double min3 = Double.MAX_VALUE;

			for (MapperResult mappingResult : values) {
				if (mappingResult.getStringKey().equals(MARKET_KEY1.toString())) {
					min1 = Math.min(min1, mappingResult.getOpening());
				} else if (mappingResult.getStringKey().equals(MARKET_KEY2.toString())) {
					min2 = Math.min(min2, mappingResult.getOpening());
				} else {
					min3 = Math.min(min3, mappingResult.getOpening());
				}
			}

			context.write(new Text(key.toString() + "_" + MARKET_KEY1.toString()), new DoubleWritable(min1));
			context.write(new Text(key.toString() + "_" + MARKET_KEY2.toString()), new DoubleWritable(min2));
			context.write(new Text(key.toString() + "_" + MARKET_KEY3.toString()), new DoubleWritable(min3));
		}
	}

	public static void main(String... args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: MinBetweenPairByYearJob <hdfs://> <in> <out>");
			System.exit(2);
		}

		FileSystem hdfs = FileSystem.get(new URI(args[0]), conf);
		Path resultFolder = new Path(args[2]);
		if (hdfs.exists(resultFolder))
			hdfs.delete(resultFolder, true);

		Job job = Job.getInstance(conf, "Minimum between pairs for each year");
		job.setJarByClass(MinByYearJob.class);
		job.setMapperClass(MinByYearMapper.class);
		job.setCombinerClass(MinByYearCombiner.class);
		job.setReducerClass(MinByYearReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MapperResult.class);

		for (int i = 1; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

		boolean finishState = job.waitForCompletion(true);
		System.out.println("Job Running Time: " + (job.getFinishTime() - job.getStartTime()));

		System.exit(finishState ? 0 : 1);
	}

}