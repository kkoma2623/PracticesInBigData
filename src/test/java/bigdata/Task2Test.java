package bigdata;

import org.apache.hadoop.util.ToolRunner;

public class Task2Test {
	public static void main(String[] not_used) throws Exception {
		String[] args = {"src/test/resources/fb-s1.txt/part-r-00000", "src/test/resources/fb-s2.out"};
		
		ToolRunner.run(new Task2(), args);
	}
}
