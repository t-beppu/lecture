import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		Text invertedIndex = new Text();
		Iterator<Text> iter = values.iterator();
		String tmp = "";
		while(iter.hasNext()){
			 tmp += iter.next().toString() + " ";
			 invertedIndex.set(tmp);
		}

		context.write(key, invertedIndex);
	}



}
