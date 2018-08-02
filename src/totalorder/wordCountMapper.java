package totalorder;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @desciption  ��ֵ���
 * @author HYT
 *
 */
public class wordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		if(StringUtils.isNotBlank(line)){
			String[] words=line.split(" ", -1);//ͨ���ո�ָ��
			if(words!=null&&words.length!=0){
				for(String tmp:words){
					if(StringUtils.isNotBlank(tmp)){//�жϵ��ʲ�Ϊ��
						Text tmpKey=new Text(tmp);
						IntWritable tmpValue=new IntWritable(1);
						context.write(tmpKey, tmpValue);
					}
				}
			}
		}
	}

}
