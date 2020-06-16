package bigdata;

import org.apache.hadoop.util.ToolRunner;

public class Task1Test {
	public static void main(String[] not_used) throws Exception {
		String[] args = {"src/test/resources/fb.txt", "src/test/resources/fb-s1.txt"};
		ToolRunner.run(new Task1(), args);
	}
}
