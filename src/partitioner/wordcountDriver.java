package partitioner;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class wordcountDriver {

	public static void main(String[] args) throws Exception {
		String intPut = "file:/D:/hadoop/workspace/partitioner/word.txt";
		String outPut ="file:/D:/hadoop/workspace/partitioner/out";
		Job job = Job.getInstance();
		job.setJarByClass(wordcountDriver.class);//��������
		job.setJobName("partitioner");//������������
		
		job.setMapperClass(wordCountMapper.class);//mapper
		job.setReducerClass(wordCountReduce.class);//reducer
		job.setMapOutputKeyClass(Text.class);//out��ʽ
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setPartitionerClass(wordCountPartitioner.class);
		job.setNumReduceTasks(2);//����Ĭ��reduce��1
		
		FileInputFormat.addInputPath(job, new Path(intPut));//��������
		FileOutputFormat.setOutputPath(job, new Path(outPut));//�������
		boolean b = job.waitForCompletion(true);//��־
		System.out.println("-----b---------");
	}
}
