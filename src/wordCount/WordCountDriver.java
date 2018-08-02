package wordCount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	
	public static void main(String[] args) throws Exception {
		//System.setProperty("HADOOP_USER_NAME", "root");
		String intputPath = "file:/D:/hadoop/workspace/wordcount/word.txt";
		String outputPath = "file:/D:/hadoop/workspace/wordcount/output";
		Job job = Job.getInstance();
		job.setJarByClass(WordCountDriver.class);

		//job.setJar("C:\\commonFiles\\vdata\\wordcount.jar");
		job.setJobName("wordcount");
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(intputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		boolean flag = job.waitForCompletion(true);
		System.out.println("-------over---------" + flag);

	}

}
