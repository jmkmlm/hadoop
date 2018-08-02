package combiner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int reducerNumber) {
		int status=0;
		String word=key.toString();//ªÒ»°µ•¥ 
		if(StringUtils.isNotBlank(word)){
			Character ch=word.charAt(0);
			boolean lowerFlag=Character.isLowerCase(ch);
			if(lowerFlag)
				status=1;
		}
		
		return status;
		
	}

}
