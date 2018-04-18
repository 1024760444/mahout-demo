package com.yhaitao.mahout.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

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
		
		Path modelPath = new Path("hdfs://172.19.10.33:8020/tmp/bayes/output/model");
		
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(modelPath, conf);
		AbstractNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
		
		// 加载DF词频信息
		Path vectorPath = new Path("hdfs://172.19.10.33:8020/tmp/bayes/urls/vectors/part-r-00000");
		for (Pair<Text, VectorWritable> record : new SequenceFileIterable<Text, VectorWritable>(vectorPath, true, conf)) {
			Vector classify = classifier.classifyFull(record.getSecond().get());
			System.err.println(record.getFirst().toString() + " : " + classify);
		}
		return 0;
	}
}
