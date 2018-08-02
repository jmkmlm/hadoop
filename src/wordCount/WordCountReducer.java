package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @description 统计单词的个数，然后写出到hdfs上
 * @author tzc
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable tmpNum:value){//累加，统计单词出现的次数
			sum+=tmpNum.get();
		}
		
		context.write(key, new IntWritable(sum));
		
	}
}
