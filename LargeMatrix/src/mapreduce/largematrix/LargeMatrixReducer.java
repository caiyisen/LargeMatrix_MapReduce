package mapreduce.largematrix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LargeMatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
	
	public static final int MATRIX_A_ROW = 10000;
    public static final int MATRIX_A_COL_B_ROW = 10000;
    public static final int MATRIX_B_COL = 10000;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int[] valA = new int[MATRIX_A_COL_B_ROW+1];
        int[] valB = new int[MATRIX_A_COL_B_ROW+1];
       
        int i;
        for (i = 1; i <= MATRIX_A_COL_B_ROW; i ++) {
             valA[i] = 0;
             valB[i] = 0;
        }
		
		for (Text val : values) {
			String line = val.toString();
			if(line.startsWith("a")) {
				String[] temp = line.split(",");
				
				valA[Integer.parseInt(temp[1])] = Integer.parseInt(temp[2]);
			}else if(line.startsWith("b")) {
				String[] temp = line.split(",");
				
				valB[Integer.parseInt(temp[1])] = Integer.parseInt(temp[2]);
				
			}
		}
		
		int result = 0;
		for( i = 1;i <= MATRIX_A_COL_B_ROW;i++) {
			result += valA[i]*valB[i];
		}
		
		context.write(key,new IntWritable(result));
	}

}
