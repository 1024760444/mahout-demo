package com.yhaitao.mahout.lookalike.job.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

public class KmeansResultMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, IntWritable, WeightedPropertyVectorWritable> {
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context) 
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
