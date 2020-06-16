package triangle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TriangleStep1 extends Configured implements Tool{
	
	public int run(String[] args) throws Exception{
		
		String input = args[0];
		String output = args[1];
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriangleStep1.class);
		
		job.setMapperClass(TS1Map.class);
		job.setReducerClass(TS1Reduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static class TS1Map extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		IntWritable ou = new IntWritable();
		IntWritable ov = new IntWritable();
		
		@Override
		protected void map(Object key, Text value, Mapper<Object,
				Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString());
			ou.set(Integer.parseInt(st.nextToken()));
			ov.set(Integer.parseInt(st.nextToken()));
			
			if(ou.get() < ov.get()) {
				context.write(ou, ov);
			} else {
				context.write(ov,  ou);
			}
		}
	}
	
	public static class TS1Reduce extends Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable>{
		
		IntPairWritable ok = new IntPairWritable();
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			List<Integer> neighbors = new ArrayList<Integer>();
			for(IntWritable v : values) {
				neighbors.add(v.get());
			}
			
			for(int i=0; i<neighbors.size(); i++) {
				for(int j=i+1; j<neighbors.size(); j++) {
					if(neighbors.get(i) < neighbors.get(j)) {
						ok.set(neighbors.get(i), neighbors.get(j));
						context.write(ok,  key);
					}
				}
			}
		}
	}

}
