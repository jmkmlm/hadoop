package cache;


import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import partitioner.wordCountReduce;

public class cacheDriver extends Configured implements Tool{
	private static final Logger LOG=Logger.getLogger(cacheDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs= new GenericOptionsParser(getConf(),args).getRemainingArgs();
		if(otherArgs.length<3) {
			System.out.println("-----��������----------");
		}
		String intputPath=otherArgs[0];
		String outputPath=otherArgs[1];
		String dicPath=otherArgs[2];
		LOG.info("----inputPath------"+intputPath);
		LOG.info("-----outputPath-------" + outputPath);
		LOG.info("-----dicPath---------" + dicPath);
		Job job = Job.getInstance();
		job.setJarByClass(cacheDriver.class);
		job.setJar("D:\\hadoop\\workspace\\cache.jar");
		job.setJobName("combiner");
		
		job.setMapperClass(cacheMapper.class);
		job.setReducerClass(cacheReduece.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
	
		Path path = new Path(dicPath);
		// # ��֮��������Ƕ������ļ������ӣ���ͬ�ļ���������������ͬ�������Զ���
		String cacheLinkPath = path.toUri().toString() + "#" + "userdic";
		DistributedCache.addCacheFile(new URI(cacheLinkPath),job.getConfiguration());
		
		FileInputFormat.addInputPath(job, new Path(intputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		int status = ToolRunner.run(new cacheDriver(), args);
		LOG.info("-----------over----------------");
		System.exit(status);
	}

	
}
