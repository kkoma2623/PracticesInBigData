package triangle;

import org.apache.hadoop.util.ToolRunner;

public class TriangleStep2Test {
	public static void main(String[] not_used) throws Exception {
		String[] args = {"src/test/resources/fb.txt", "src/test/resources/fb-s1.txt", "src/test/resources/fb.out"};
		
		ToolRunner.run(new TriangleStep2(), args);
	}

}
