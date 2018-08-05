package lagou;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapLagou extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Map<String, String> dicMap = null;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		BufferedReader buffer = null;
		try {
			buffer = new BufferedReader(new InputStreamReader(new FileInputStream("D:/hadoop/lagoudic")));
			if (buffer != null) {
				dicMap = new HashMap<String, String>();
				while (buffer.ready()) {
					String line = buffer.readLine();
					if (StringUtils.isNotBlank(line)) {
						String[] lines = line.split(",", -1);
						if (lines != null && lines.length == 2) {
							dicMap.put(lines[0], line);
						}
					}
				}
			}

		} catch (Exception e) {

		} finally {
			try {
				if (buffer != null)
					buffer.close();
			} catch (Exception e) {

			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (StringUtils.isNotBlank(line)) {
			String[] lines = line.split(",");
			if (lines != null && lines.length >= 6) {
				int len = lines.length;
				String rongzi = null;
				String address = null;
				if (len == 6) {
					rongzi = lines[2];
					address = lines[4];
				} else if (len == 7) {
					rongzi = lines[3];
					address = lines[5];
				}

				// 将融资情况写出去
				if (StringUtils.isNotBlank(rongzi)) {
					context.write(new Text("first," + rongzi), new IntWritable(1));
				}

				// 提取城区
				String areaName = getAreaName(address);
				if (StringUtils.isNotBlank(areaName)) {
					context.write(new Text("seconde," + areaName), new IntWritable(1));
				}
			}
		}
	}

	private String getAreaName(String address) {
		String areaName = null;
		if (StringUtils.isNotBlank(address) && address.contains("北京")) {
			if (dicMap != null && dicMap.size() != 0) {
				Set<String> keySet = dicMap.keySet();// 汉字map
				for (String tmpKey : keySet) {
					if (address.contains(tmpKey)) {
						areaName = dicMap.get(tmpKey);// 得到区域信息---key得到value
					}
				}
			}
		}
		return areaName;// 完整字段 中英文
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

}
