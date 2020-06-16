package triangle;

import org.apache.hadoop.util.ToolRunner;

public class TriangleStep1Test {
	public static void main(String[] notused) throws Exception{
		String[] args = {"src/test/resources/fb.txt","src/test/resources/fb-s1.txt"};
		ToolRunner.run(new TriangleStep1(), args);
	}
}
