package day6_homework;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends Reducer<WordCountWrite, Text, Text, Text> {

	@Override
	protected void reduce(WordCountWrite key, Iterable<Text> value,
			Reducer<WordCountWrite, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {

		for(Text tmpNum:value){
			context.write(new Text(key.toString()), tmpNum);
		}
	}
}
