package day6_homework;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		String intputPath = "/workspace/hyt/day6/homework/zonghe";
		String outputPath = "/workspace/hyt/day6/homework/output2";
		Job job = Job.getInstance();
		job.setJarByClass(WordCountDriver.class);

		job.setJar("D:\\hadoop\\workspace\\zonghe.jar");
		job.setJobName("zonghe");
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(WordCountWrite.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(WordCountPartition.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(intputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean flag = job.waitForCompletion(true);
		System.out.println("-------over---------" + flag);

	}

}
