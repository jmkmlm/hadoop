package lagou;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RedLagou extends Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> out = null;

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.out = new MultipleOutputs<>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String tmpKey = key.toString();
		if (StringUtils.isNotBlank(tmpKey)) {
			String[] tmpKeys = tmpKey.split(",", -1);
			int sum = 0;
			for (IntWritable tmp : value) {
				sum += tmp.get();
			}
			if (tmpKey.startsWith("first")) {// 处理第一个需求
				String newTmpKey=tmpKeys[1];
				String keyFlag = newTmpKey.substring(0, 1);
				out.write(new Text(newTmpKey), new IntWritable(sum), "rongzi/" + keyFlag);
			} else {
				String areaNamech = tmpKeys[1];
				String areaNameen = tmpKeys[2];
				out.write(new Text(areaNamech), new IntWritable(sum), "city/" + areaNameen + "/");
			}

		}
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if (out != null)
			out.close();
	}
}
