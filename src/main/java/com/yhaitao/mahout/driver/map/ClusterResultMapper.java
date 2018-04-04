package com.yhaitao.mahout.driver.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;

public class ClusterResultMapper extends Mapper<IntWritable, WeightedVectorWritable, IntWritable, WeightedVectorWritable> {
	protected void map(IntWritable key, WeightedVectorWritable value, Context context) 
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
