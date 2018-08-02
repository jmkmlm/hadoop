package totalorder;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @desciption Í³¼Æµ¥´Ê
 * @author HYT
 *
 */
public class wordCountReduce extends Reducer<Text, IntWritable,Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> Values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable tmp:Values) {
			sum+=tmp.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
