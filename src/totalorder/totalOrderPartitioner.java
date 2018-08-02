package totalorder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class totalOrderPartitioner extends TotalOrderPartitioner<Text,IntWritable> {

}
