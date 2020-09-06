package bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task2 extends Configured implements Tool{
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Task2(), args);
	}
	public int run(String[] args) throws Exception {
		
		String input = args[0];
		String output = args[1];
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task2.class);
		
		job.setMapperClass(T2Map.class);
		job.setReducerClass(T2Reduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static class T2Map extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		IntWritable ou = new IntWritable();
		IntWritable ov = new IntWritable();
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString());
			ou.set(Integer.parseInt(st.nextToken()));
			ov.set(Integer.parseInt(st.nextToken()));
			
			IntWritable one = new IntWritable(1);
			
			context.write(ou, one);
			context.write(ov, one);
		}
	}
	
	public static class T2Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		
		IntWritable count = new IntWritable();
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			Integer sum = 0;
			
			for(IntWritable value : values) {
				sum += value.get();
			}
			count.set(sum);
			
			if(count.get() > 0) context.write(key, count);
		}
	}
}
