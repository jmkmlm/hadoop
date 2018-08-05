package day6_homework;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartition extends Partitioner<WordCountWrite, Text> {

	@Override
	public int getPartition(WordCountWrite key, Text value, int num) {
		int status=0;
		String word = value.toString();
		if(StringUtils.isNotBlank(word)) {
			if("man".equals(word)) {
				status=1;
			}
		}
		return status;

	}

}
