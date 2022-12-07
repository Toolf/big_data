import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class AverageCount {

	public static class AverageCountMapper extends Mapper<Object, Text, Text, Double>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String csvLine = value.toString();
			String[] csvField = csvLine.split(",");
			
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
			LocalDate year1 = LocalDate.parse(csvField[0]);
			LocalDate year2 = LocalDate.parse(csvField[0+8]);

			context.write(new Text(String.valueOf(year1.getYear())), Double.parseDouble(csvField[2]));
			context.write(new Text(String.valueOf(year2.getYear())), Double.parseDouble(csvField[2+8]));
		}
	 }

	 public static class AverageCountReducer extends Reducer<Text, Double, Text, Double>{
		 
		public void reduce(Text key, Iterable<Double> values, Context context) throws IOException, InterruptedException {
			Double minValue = null;
			for(Double value: values) {
				if (minValue == null || minValue > value) {
					minValue = value;
				}
			}
			if (minValue != null) {
				context.write(key, minValue);
			}
		}

	 }

	 public static void main(String... args) throws Exception{

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: AverageCount <hdfs://> <in> <out>");
			System.exit(2);
		}

		FileSystem hdfs=FileSystem.get(new URI(args[0]), conf);
		Path resultFolder=new Path(args[2]);
		if(hdfs.exists(resultFolder))
			hdfs.delete(resultFolder, true);

		Job job = Job.getInstance(conf, "Market Average Count");
		job.setJarByClass(AverageCount.class);
		job.setMapperClass(AverageCountMapper.class);
		job.setCombinerClass(AverageCountReducer.class);
		job.setReducerClass(AverageCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		for (int i = 1; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

		boolean finishState = job.waitForCompletion(true);
		System.out.println("Job Running Time: " + (job.getFinishTime() - job.getStartTime()));

		System.exit(finishState ? 0 : 1);
	 }

}
