package lianjiaDemo1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class reduceLianjia extends Reducer< Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> out = null;
	
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		out = new MultipleOutputs<>(context);
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		String line = key.toString();
		if(StringUtils.isNotBlank(line)) {
			String[] lines = line.split(",");
			int sum=0;
			for(IntWritable tmpValue:value) {
				sum+=tmpValue.get();
			}

			if(lines!=null&&lines.length==2) {
				//不同城区 小区  分布情况
				String city = lines[0];
				String area = lines[1];
				out.write(new Text(area), new IntWritable(sum), "homecount/"+city);
			}else {
				//不同城区 楼型 关注度
				String city = lines[0];
				String floor = lines[1];
				String atten = lines[2];
				out.write(new Text(floor), new IntWritable(Integer.parseInt(atten)),"homecount/"+city);
			}
			
			
				
	
		}
	}
}
