package lianjiaDemo1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driverlainjia {

	public static void main(String[] args) throws Exception {
		String intput="/workspace/hyt/lianjia/intput/lianjia.csv";
		String output="/workspace/hyt/lianjia/output";
		
		Job job = Job.getInstance();
		job.setJarByClass(Driverlainjia.class);
		job.setJobName("lianjia1");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(maplianjia.class);
		job.setReducerClass(reduceLianjia.class);
		
		FileInputFormat.addInputPath(job, new Path(intput));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		boolean b = job.waitForCompletion(true);
		System.out.println("---------"+b);
	
	}
}
