package com.yhaitao.mahout.lookalike.job.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class To11VectorReducer extends Reducer<LongWritable, Text, LongWritable, VectorWritable> {
	/**
	 * 输入： 
	 * key : 用户标识
	 * value :  特征名称	特征对象
	 * 输出：
	 * key ： 用户标识
	 * value ： 用户特征向量
	 */
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Reducer<LongWritable, Text, LongWritable, VectorWritable>.Context context)
			throws IOException, InterruptedException {
		// 特征权重统计
		Iterator<Text> iterator = values.iterator();
		Map<String, Integer> featureWeight = new HashMap<String, Integer>();
		while(iterator.hasNext()) {
			Text next = iterator.next();
			String string = next.toString();
			String[] split = string.split("\t");
			if(split != null && split.length >= 2) {
				if(featureWeight.containsKey(split[0])) {
					featureWeight.put(split[0], featureWeight.get(split[0]) + 1);
				} else {
					featureWeight.put(split[0], 1);
				}
			}
		}
		
		// 组建特征向量
		NamedVector namedVector = new NamedVector(toVector(featureWeight), key.toString());
		VectorWritable vectorWritable = new VectorWritable(namedVector);
		context.write(key, vectorWritable);
	}
	
	/**
	 * 按格式组建特征向量： interest1 interest2 interest3 interest4 interest5 kw1 kw2 kw3 topic1 topic2 topic3
	 * @param featureWeight 特征向量权重统计
	 * @return 特征向量
	 */
	private Vector toVector(Map<String, Integer> featureWeight) {
		Vector vector = new RandomAccessSparseVector(11,11);
		int index = 0;
		vector.set(index++, featureWeight.containsKey("interest1") ? featureWeight.get("interest1") : 0);
		vector.set(index++, featureWeight.containsKey("interest2") ? featureWeight.get("interest2") : 0);
		vector.set(index++, featureWeight.containsKey("interest3") ? featureWeight.get("interest3") : 0);
		vector.set(index++, featureWeight.containsKey("interest4") ? featureWeight.get("interest4") : 0);
		vector.set(index++, featureWeight.containsKey("interest5") ? featureWeight.get("interest5") : 0);
		vector.set(index++, featureWeight.containsKey("kw1") ? featureWeight.get("kw1") : 0);
		vector.set(index++, featureWeight.containsKey("kw2") ? featureWeight.get("kw2") : 0);
		vector.set(index++, featureWeight.containsKey("kw3") ? featureWeight.get("kw3") : 0);
		vector.set(index++, featureWeight.containsKey("topic1") ? featureWeight.get("topic1") : 0);
		vector.set(index++, featureWeight.containsKey("topic2") ? featureWeight.get("topic2") : 0);
		vector.set(index++, featureWeight.containsKey("topic3") ? featureWeight.get("topic3") : 0);
		return vector;
	}
}
