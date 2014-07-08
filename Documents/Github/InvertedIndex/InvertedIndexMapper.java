import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text word = new Text();
	private Text index = new Text();
	int row = 0;
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		StringTokenizer tokenizer = new StringTokenizer(value.toString());//行ごとに分割
		String position = "";
			int counter = 0;
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				position = "(" + row + ", " + counter +")";
				index.set(position);
				context.write(word, index);
				counter += 1 + word.toString().length();
			}
			row  += 1;
	}


}
