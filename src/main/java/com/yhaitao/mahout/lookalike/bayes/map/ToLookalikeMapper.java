package com.yhaitao.mahout.lookalike.bayes.map;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.vectorizer.TFIDF;

import com.yhaitao.mahout.utils.CSVUtils;

public class ToLookalikeMapper extends Mapper<Text, Text, Text, Text> {
	/**
	 * 分词编码字典
	 */
	private OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<>();

	/**
	 * 分词DF词频
	 */
	private OpenIntLongHashMap frequency = new OpenIntLongHashMap();
	
	private Map<String, Integer> labelIndexMap = new HashMap<String, Integer>();

	/**
	 * tfidf计算工具
	 */
	private TFIDF tfidf = new TFIDF();
	
	private AbstractNaiveBayesClassifier classifier;
	
	/**
	 * 1 初始化分词工具 2 加载分词词典
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// 加载分词编码字典
		Path dictionaryFile = new Path("hdfs://172.19.10.33:8020/tmp/lookalike/bayes/output/dictionary.file-0");
		for (Pair<Writable, IntWritable> record : new SequenceFileIterable<Writable, IntWritable>(
				dictionaryFile, true, conf)) {
			dictionary.put(record.getFirst().toString(),
					record.getSecond().get());
		}
		
		// 加载DF词频信息
		Path frequencyFile = new Path("hdfs://172.19.10.33:8020/tmp/lookalike/bayes/output/frequency.file-0");
		for (Pair<IntWritable, LongWritable> record : new SequenceFileIterable<IntWritable, LongWritable>(
				frequencyFile, true, conf)) {
			frequency.put(record.getFirst().get(), record.getSecond().get());
		}
		
		// 分类编号
		Path classItem = new Path("hdfs://172.19.10.33:8020/tmp/lookalike/bayes/output/labelIndex");
		for (Pair<Text, IntWritable> record : new SequenceFileIterable<Text, IntWritable>(
				classItem, true, conf)) {
			labelIndexMap.put(record.getFirst().toString(), record.getSecond().get());
		}
		
		// 分类模型
		Path modelPath = new Path("hdfs://172.19.10.33:8020/tmp/lookalike/bayes/output/model");
		NaiveBayesModel naiveBayesModel = NaiveBayesModel.materialize(modelPath, conf);
		this.classifier = new StandardNaiveBayesClassifier(naiveBayesModel);
	}
	
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// 抓取、过滤、ik
		String webUrl = key.toString();
		String string = value.toString();
		String[] termList = string.split(" ");
		// List<String> termList = value.getEntries();

		// 组建TF向量
		int size = dictionary.size();
		Vector vector = new RandomAccessSparseVector(size, size);
		for (String termName : termList) {
			if (null != termName && !"".equals(termName)
					&& dictionary.containsKey(termName)) {
				int termId = dictionary.get(termName);
				vector.setQuick(termId, vector.getQuick(termId) + 1);
			}
		}

		// TFIDF计算
		Vector tfidfVector = new RandomAccessSparseVector(size, size);
		for (Vector.Element e : vector.nonZeroes()) {
			if (!frequency.containsKey(e.index())) {
				continue;
			}
			long df = frequency.get(e.index());
			if (df < 1) {
				df = 1;
			}
			
			// TF-IDF = TF * log(46248/df)
			tfidfVector.setQuick(e.index(), tfidf.calculate((int) e.get(), (int) df, (int) 1, size));
		}

		// 名称向量，输出。
		Vector classifyFull = this.classifier.classifyFull(tfidfVector);
		String[] splitKeyInfo = webUrl.split("\\/");
		int integer = labelIndexMap.get(splitKeyInfo[1]);
		double d = classifyFull.get(integer);
		double format = CSVUtils.format(d);
		context.write(new Text(new StringBuffer()
				.append(splitKeyInfo[1]).append(",")
				.append(splitKeyInfo[2]).append(",")
				.append(format)
				.toString()), null);
		// tfidfVector = new NamedVector(tfidfVector, webUrl);
		// VectorWritable vectorWritable = new VectorWritable(tfidfVector);
		// context.write(new Text(webUrl), vectorWritable);
	}
}
