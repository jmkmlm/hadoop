package student;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @description ÇóºÍ
 * @author HYT
 *
 */
public class studentReducer extends Reducer<studentWritable, IntWritable, Text, IntWritable> {


	@Override
	protected void reduce(studentWritable key, Iterable<IntWritable> values,
			Reducer<studentWritable, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable value:values) {
			sum+=value.get();
		}
		String line = key.toString();
		context.write(new Text(line), new IntWritable(sum));
	}
}
