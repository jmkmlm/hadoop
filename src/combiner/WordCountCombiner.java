package combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @description ���й�Լ���� �������紫��
 * @author HYT
 *
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable tmp:values) {
			sum+=tmp.get();
		}
		sum=sum*10;
		context.write(key, new IntWritable(sum));
		
	}
}
