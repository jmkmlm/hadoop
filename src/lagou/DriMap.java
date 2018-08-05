package lagou;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DriMap {

	public static void main(String[] args) throws Exception {
		String intput="/workspace/hyt/lagou/input/lagou.txt";
		String output="/workspace/hyt/lagou/output2";
		
		Job job = Job.getInstance();
		job.setJarByClass(DriMap.class);
		job.setJobName("lagou2");
		
		job.setMapperClass(MapLagou.class);
		job.setReducerClass(RedLagou.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//添加区域字典
		DistributedCache.addCacheFile(new URI("file:/D:/hadoop/lagoudic"), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(intput));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		boolean b = job.waitForCompletion(true);
		System.out.println("---------"+b);
	}
}
