package triangle;

import org.apache.hadoop.util.ToolRunner;

public class TriangleListing {
	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		String tmp = output + ".tmp";
		
		ToolRunner.run(new TriangleStep1(), new String[] {input, tmp});
		ToolRunner.run(new TriangleStep2(), new String[] {input, tmp, output});
		
	}

}
