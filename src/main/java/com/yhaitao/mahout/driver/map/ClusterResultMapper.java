package com.yhaitao.mahout.driver.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

public class ClusterResultMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, IntWritable, WeightedPropertyVectorWritable> {
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context) 
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
