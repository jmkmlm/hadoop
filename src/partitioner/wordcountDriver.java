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
		job.setJarByClass(wordcountDriver.class);//加载主类
		job.setJobName("partitioner");//设置任务名字
		
		job.setMapperClass(wordCountMapper.class);//mapper
		job.setReducerClass(wordCountReduce.class);//reducer
		job.setMapOutputKeyClass(Text.class);//out格式
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setPartitionerClass(wordCountPartitioner.class);
		job.setNumReduceTasks(2);//由于默认reduce是1
		
		FileInputFormat.addInputPath(job, new Path(intPut));//输入序列
		FileOutputFormat.setOutputPath(job, new Path(outPut));//输出序列
		boolean b = job.waitForCompletion(true);//日志
		System.out.println("-----b---------");
	}
}
