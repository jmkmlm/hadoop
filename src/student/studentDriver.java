package student;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class studentDriver {

	public static void main(String[] args) throws Exception {
		String intputPath="file:/D:/hadoop/workspace/student/studentdata.txt";
		String outputPath="file:/D:/hadoop/workspace/student/out";
		Job job = Job.getInstance();
		job.setJarByClass(studentDriver.class);
		
		job.setJobName("我是傻逼少点九棘鲈");
		job.setMapperClass(studentMapper.class);
		job.setReducerClass(studentReducer.class);
		
		job.setMapOutputKeyClass(studentWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(intputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		boolean b = job.waitForCompletion(true);
		System.out.println("------b-----over----");
}
}
