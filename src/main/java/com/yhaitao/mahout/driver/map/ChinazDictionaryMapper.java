package com.yhaitao.mahout.driver.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yhaitao.mahout.driver.bean.ChinazDictionary;

public class ChinazDictionaryMapper extends Mapper<Text, IntWritable, ChinazDictionary, NullWritable> {
	protected void map(Text key, IntWritable value, Context context) 
			throws IOException, InterruptedException {
		context.write(new ChinazDictionary(value.get(), key.toString()), NullWritable.get());
	}
}
