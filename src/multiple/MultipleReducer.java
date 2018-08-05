package multiple;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
/**
 * @descriptipn 多目录输出主要在reduce输出端
 * @author HYT
 *
 */
public class MultipleReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> out = null;
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.out = new MultipleOutputs<>(context);//将reduce输出端格式化
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String line = key.toString();
		if (StringUtils.isNotBlank(line)) {
			String[] lines = line.split(",",-1);
			if(lines!=null && lines.length>=2) {
				String gender = lines[1];
				int sum=0;
				for(IntWritable tmp:values){
					sum+=tmp.get();
				}
				//多目录输出
				//out.write(key, new IntWritable(sum), gender+"/");
				if (gender.equals("man")) {
					this.out.write("text", key, new IntWritable(sum),
							gender + "/");
				} else {
					this.out.write("seq", key, new IntWritable(sum), gender
							+ "/");
				}
			}
		}
	}
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if(out!=null) 
			out.close();
	}
}
