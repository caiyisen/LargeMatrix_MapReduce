package test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateMatrix {
	public static void main(String args[]) {
		FileWriter writer;
		
        try {
            writer = new FileWriter("E:\\share\\input\\Matrix_B.txt");
            
            for(int i=1;i<=10000;i++) {
            	for(int j=1;j<=10000;j++) {
            		Random random = new Random();
            		int number = random.nextInt(10);
            		if(number == 0) continue;
            		String num = Integer.toString(number);
            		String row = Integer.toString(i);
            		String col = Integer.toString(j);
            		String line = row+","+col+","+num+"\n";
            		writer.write(line);
            		
            	}
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
}
