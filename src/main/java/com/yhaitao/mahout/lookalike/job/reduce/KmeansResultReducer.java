package com.yhaitao.mahout.lookalike.job.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

/**
 * 输出分类信息到Mysql。
 * @author yanghaitao
 *
 */
public class KmeansResultReducer extends Reducer<IntWritable, WeightedPropertyVectorWritable, Text, Text> {
	/**
	 * 输入： 
	 * key ： 聚类编号
	 * value ： 聚类向量信息
	 * 输出：
	 * key : 分类标识
	 * value : 用户标识
	 */
	protected void reduce(IntWritable key, Iterable<WeightedPropertyVectorWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<WeightedPropertyVectorWritable> iterator = values.iterator();
		while(iterator.hasNext()) {
			WeightedPropertyVectorWritable next = iterator.next();
			NamedVector namedVector = (NamedVector) next.getVector();
			
			// 保存域名与聚类编号的关系
			String name = namedVector.getName();
			context.write(new Text(key.toString()), new Text(name));
		}
	}
}
