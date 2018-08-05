package multiple;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MultipleDriver extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(MultipleDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length < 2) {
			LOG.info("----------参数不对-----------------");
		}

		String intputPath = otherArgs[0];
		String outputPath = otherArgs[1];
		// String dicPath = otherArgs[2];
		LOG.info("-----inputPath-------" + intputPath);
		LOG.info("-----outputPath-------" + outputPath);
		// LOG.info("-----dicPath---------" + dicPath);

		Job job = Job.getInstance();
		job.setJarByClass(MultipleDriver.class);

		job.setJar("D:\\hadoop\\workspace\\multipleDemo.jar");
		job.setJobName("multipleDemo");
		job.setMapperClass(MultipleMapper.class);
		job.setReducerClass(MultipleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Path path = new Path(dicPath);
		// # 号之后的名称是对上面文件的链接，不同文件的链接名不能相同，名称自定义
		// String cacheLinkPath = path.toUri().toString()+"#userdic";
		DistributedCache.addCacheFile(new URI("file:/D:/hadoop/userdic"), job.getConfiguration());

		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, Text.class, IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(intputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		int status = ToolRunner.run(new MultipleDriver(), args);
		LOG.info("-----------over----------------");
		System.exit(status);

	}

}
