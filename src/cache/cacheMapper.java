package cache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class cacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Logger LOG=Logger.getLogger(cacheMapper.class);
	private Map<String, String> dicMap=null;//字典表
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {//map任务初始化调用

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream("userdic")));
			if(br!=null) {
				dicMap = new HashMap<String,String>();
				while (br.ready()) {
					String line = br.readLine();
					if (StringUtils.isNotBlank(line)) {
						String[] lines = line.split(",", -1);
						if (lines != null && lines.length != 0) {
							dicMap.put(lines[0], lines[1] + "," + lines[2]);//切割字段
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error(e);
		}finally {
			try {
				if(br!=null) {
					br.close();
				}
			} catch (Exception e) {
				LOG.error(e);
			}
		}

	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (StringUtils.isNotBlank(line)) {
			String[] lines = line.split(",", -1);
			if (lines != null && lines.length != 0) {
				String userCode = lines[0];
				if (dicMap != null && dicMap.size() != 0) {
					String userMessage = dicMap.get(userCode);// 通过字典表关联用户信息
					if (StringUtils.isNotBlank(userMessage))
						context.write(new Text(userMessage), new IntWritable(1));
				}
			}
		}

	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {//map任务结束调用
		super.cleanup(context);
	}
}
