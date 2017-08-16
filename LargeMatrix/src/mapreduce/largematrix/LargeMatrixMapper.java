package mapreduce.largematrix;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LargeMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public static final int MATRIX_A_ROW = 10000;
    public static final int MATRIX_A_COL_B_ROW = 10000;
    public static final int MATRIX_B_COL = 10000;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String path = ((FileSplit)context.getInputSplit()).getPath().toString();
		
		if(path.contains("Matrix_A")) {
			String line = value.toString();
			
			if(line == null || line.equals("")) return;
			
			String []values = line.split(",");
			
			if(values.length <3 ) return;
			
			String rowindex = values[0];
			String colindex = values[1];
			String pvalue = values[2];
			
			for(int i = 1;i <= MATRIX_B_COL;i++) {
				context.write(new Text(rowindex+","+i), new Text("a"+","+colindex+","+pvalue));
			}
			
		}
		if(path.contains("Matrix_B")) {
				String line = value.toString();
				
				if(line == null || line.equals("")) return;
				
				String []values = line.split(",");
				
				if(values.length <3 ) return;
				
				String rowindex = values[0];
				String colindex = values[1];
				String pvalue = values[2];
				
				for(int i = 1;i <= MATRIX_A_ROW;i++) {
					context.write(new Text(i+","+colindex), new Text("b"+","+rowindex+","+pvalue));
				}
			
		}
	}

}
