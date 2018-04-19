package com.yhaitao.mahout.lookalike.job.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class To11VectorMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	/**
	 * 输入：用户最大粒度特征信息
	 * key ： 序号
	 * value ： 用户标识	特征名称	特征对象
	 * 输出： 
	 * key ： 用户标识
	 * value ： 特征名称	特征对象
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// 切分参数以及校验
		String string = value.toString();
		String[] split = string.split("\t");
		if(split == null || split.length < 3) {
			return ;
		}
		
		// 输出
		long uid = Long.valueOf(split[0]);
		String iname = split[1].trim();
		String ivalue = split[2].trim();
		
		if(iname.startsWith("interest1")
				|| iname.startsWith("interest2")
				|| iname.startsWith("interest3")
				|| iname.startsWith("interest4")
				|| iname.startsWith("interest5")
				|| iname.startsWith("kw1")
				|| iname.startsWith("kw2")
				|| iname.startsWith("kw3")
				|| iname.startsWith("topic1")
				|| iname.startsWith("topic2")
				|| iname.startsWith("topic3")) {
			context.write(new LongWritable(uid), new Text(iname + "\t" + ivalue));
		}
	}
}
