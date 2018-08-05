package Writeable;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description �ָ��ַ�
 * @author HYT
 *
 */
public class studentMapper extends Mapper<LongWritable, Text, studentWritable, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, studentWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(StringUtils.isNotBlank(line)) {
			String[] lines = line.split(",");
			for(String tmp:lines) {
				String name = lines[0];
				String gender=lines[1];
				String number = lines[2];
			
				studentWritable tmpKey = new studentWritable(new Text(name),new Text(gender));
				IntWritable tmpValue = new IntWritable(Integer.parseInt(number));
				context.write(tmpKey, tmpValue);
			
			}
		}
	}
}
