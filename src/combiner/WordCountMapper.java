package combiner;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description 閫氳繃map闃舵杩涜鍗曡瘝鍒囧垎
 * @author tzc
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		if(StringUtils.isNotBlank(line)){
			String[] words=line.split(" ", -1);//閫氳繃绌烘牸鍒嗗壊鍗曡瘝
			if(words!=null&&words.length!=0){
				
				for(String tmp:words){
					if(StringUtils.isNotBlank(tmp)){//鍒ゆ柇鍗曡瘝涓嶄负绌�
						Text tmpKey=new Text(tmp);
						IntWritable tmpValue=new IntWritable(1);
						context.write(tmpKey, tmpValue);
					}
				}
			}
		}
	}
}
