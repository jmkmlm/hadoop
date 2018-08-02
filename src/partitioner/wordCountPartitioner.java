package partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class wordCountPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int reducerNumber) {
		int status=0;
		String word = key.toString();
		if(StringUtils.isNotBlank(word)) {
			Character ch = word.charAt(0);
			boolean lowerFlag = Character.isLowerCase(ch);
			if(lowerFlag) {
				status =1;
			}
		}
		return status;
	}

}
