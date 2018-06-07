package com.yhaitao.mahout.lookalike.bayes.map;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.yhaitao.mahout.utils.CSVUtils;

public class PackVectorMapper extends Mapper<Text, VectorWritable, Text, VectorWritable> {
	@Override
	protected void map(Text key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		Vector vector = value.get();
		Vector newVector = vector.clone();
		Iterable<Element> nonZeroes = newVector.nonZeroes();
		Iterator<Element> iterator = nonZeroes.iterator();
		while(iterator.hasNext()) {
			Element next = iterator.next();
			int index = next.index();
			double d = next.get();
			newVector.set(index, CSVUtils.format(d));
		}
		
		VectorWritable vectorWritable = new VectorWritable(newVector);
		context.write(key, vectorWritable);
	}
}
