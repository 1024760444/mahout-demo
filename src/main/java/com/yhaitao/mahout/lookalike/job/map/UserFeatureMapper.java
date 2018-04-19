package com.yhaitao.mahout.lookalike.job.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 解析用户特征。
 * @author yhaitao
 *
 */
public class UserFeatureMapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
	 * 输入
	 * key ： 行号
	 * value ： 每行代表一个用户特征信息。
	 * 输出：
	 * key : 用户标识
	 * value : 特征名称 | 特征标识
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 拆分特征
		String userData = value.toString();
		String[] split = userData.split("\\|");
		
		// 最细粒度，组合特征
		String uid = null;
		List<String> featureList = new ArrayList<String>();
		if(null != split && split.length > 0) {
			for(String ufeature : split) {
				if(StringUtils.isNotBlank(ufeature)) {
					String[] userInfo = ufeature.split(" ");
					int size = userInfo.length;
					if(userInfo != null && size >= 2) {
						// 获取用户标识
						if(ufeature.startsWith("uid ")) {
							uid = userInfo[1].trim();
						}
						// 其他特征获取
						else {
							for(int index = 1; index < size; index++) {
								featureList.add(userInfo[0] + "\t" + userInfo[index]);
							}
						}
					}
				}
			}
		}
		
		// 输出最小粒度用户特征
		if(StringUtils.isNotBlank(uid) && !featureList.isEmpty()) {
			for(String data : featureList) {
				context.write(new Text(uid), new Text(data));
			}
		}
	}
}
