package com.yhaitao.mahout.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * 贝叶斯分类服务。
 * @author yhaitao
 *
 */
public class BayesClassifierServer extends AbstractJob {
	/**
	 * 启动服务器
	 * @param args 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new BayesClassifierServer(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(getConf());
		
//		conf.set("fs.defaultFS", "hdfs://172.19.10.33:9000/");
		Path modelPath = new Path("/tmp/bayes/output/model");
		
		Vector vector = new RandomAccessSparseVector(3);
		vector.set(0, 0.2);
		vector.set(1, 0.3);
		vector.set(2, 0.4);
		
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(modelPath, conf);
		AbstractNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		Vector classify = classifier.classifyFull(vector);
		System.out.println(classify);
		return 0;
	}
	
}
