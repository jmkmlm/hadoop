package day6_homework;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, WordCountWrite, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, WordCountWrite, Text>.Context context)
					throws IOException, InterruptedException {
		String line=value.toString();
		if(StringUtils.isNotBlank(line)){
			String[] words=line.split(",", -1);
			if(words!=null&&words.length>=3){
				String name = words[0];
				String age = words[1];
				String gender=words[2];
				WordCountWrite tmpKey=new WordCountWrite(new Text(name),new Text(age));
				context.write(tmpKey, new Text(gender));
			}
		}
	}
}
