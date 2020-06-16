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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import triangle.TriangleStep1.TS1Map;
import triangle.TriangleStep1.TS1Reduce;

public class TriangleStep2 extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		String edgeInput = args[0];
		String wedgeInput = args[1];
		String output = args[2];
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriangleStep2.class);
		
//		job.setMapperClass(TS1Map.class);
		job.setReducerClass(TS2Reduce.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
//		job.setInputFormatClass(TextInputFormat.class); this is default format
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(wedgeInput), SequenceFileInputFormat.class, TS2WedgeMap.class);
		MultipleInputs.addInputPath(job, new Path(edgeInput), TextInputFormat.class, TS2EdgeMap.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	
	//	input is fb-s1.txt directory
	public static class TS2WedgeMap extends Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
		
		@Override
		protected void map(IntPairWritable key, IntWritable value,
				Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	//	input is fb.txt
	public static class TS2EdgeMap extends Mapper<Object, Text, IntPairWritable, IntWritable>{
		
		IntPairWritable ok = new IntPairWritable();
		IntWritable minusOne = new IntWritable(-1);
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntPairWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString());
			int u = Integer.parseInt(st.nextToken());
			int v = Integer.parseInt(st.nextToken());
			
			if(u < v) {
				ok.set(u, v);
			} else {
				ok.set(v, u);
			}
			
			context.write(ok, minusOne);
		}
	}
	
	public static class TS2Reduce extends Reducer<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
		
		IntWritable ov = new IntWritable();
		
		@Override
		protected void reduce(IntPairWritable key, Iterable<IntWritable> values,
				Reducer<IntPairWritable, IntWritable, IntPairWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			List<Integer> nodes = new ArrayList<Integer>();
			boolean minusOne = false;
			for(IntWritable v : values) {
				if(v.get() == -1) {
					minusOne = true;
				}else {
					nodes.add(v.get());
				}
				
				if(minusOne) {
					for(int n : nodes) {
						ov.set(n);
						context.write(key, ov);
					}
				}
			}
		}
	}

}
