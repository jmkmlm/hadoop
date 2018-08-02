package combiner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import partitioner.wordCountReduce;

public class WordCountNewDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs= new GenericOptionsParser(getConf(),args).getRemainingArgs();
		if(otherArgs.length<2) {
			System.out.println("-----参数不对----------");
		}
		String intputPath=args[0];
		String outputPath=args[1];
		Job job = Job.getInstance();
	
		job.setJobName("combiner");
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
		return 0;
	}
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		int status = ToolRunner.run(new WordCountNewDriver(), args);
		System.exit(status);
	}

	
}
