package cache_hdfs;


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

public class CacheDriver extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CacheDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();
		if (otherArgs.length < 3) {
			LOG.info("----------参数不对-----------------");
		}

		String intputPath = otherArgs[0];
		String outputPath = otherArgs[1];
		String dicPath = otherArgs[2];
		LOG.info("-----inputPath-------" + intputPath);
		LOG.info("-----outputPath-------" + outputPath);
		LOG.info("-----dicPath---------" + dicPath);

		Job job = Job.getInstance();
		job.setJarByClass(CacheDriver.class);

		//job.setJar("D:\\hadoop\\workspace\\cache.jar");
		job.setJobName("cacheDemo");
		job.setMapperClass(cacheMapper.class);
		job.setReducerClass(cacheReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path path = new Path(dicPath);
		// # 号之后的名称是对上面文件的链接，不同文件的链接名不能相同，名称自定义
		String cacheLinkPath = path.toUri().toString()+"#" + "userdic";
		System.out.println(cacheLinkPath);
		DistributedCache.addCacheFile(new URI(cacheLinkPath),
				job.getConfiguration());

		FileInputFormat.addInputPath(job, new Path(intputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//System.setProperty("HADOOP_USER_NAME", "root");
		int status = ToolRunner.run(new CacheDriver(), args);
		LOG.info("-----------over----------------");
		System.exit(status);

	}

}
