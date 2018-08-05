package lianjiaDemo1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class maplianjia extends Mapper<LongWritable, Text, Text, IntWritable> {



	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(StringUtils.isNotBlank(line)) {
			String[] lines = line.split("#");
			if(lines!=null&&lines.length>=9) {

				String city= lines[1];//命名文件
				String area = lines[2];
				String attention = lines[3];
				String floor = lines[7];


				//不同城区 小区  分布情况
				String areaCount = city+","+area;
				IntWritable tmpValue = new IntWritable(1);
				context.write(new Text(areaCount), tmpValue);
				//不同城区 楼型 关注度
				String floorAttention = city+","+floor+","+attention;
				context.write(new Text(floorAttention), tmpValue);
			}
		}



	}
}
