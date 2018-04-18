package com.yhaitao.mahout.driver.map;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.vectorizer.TFIDF;

/**
 * 构建向量。 抓取、过滤网页，获取文本 IK分词获取，词频统计TF 单词编码 根据DF计算向量（tfidf）
 * 
 * @author admin
 *
 */
public class Text2VectorMapper
		extends
			Mapper<Text, StringTuple, Text, VectorWritable> {
	/**
	 * 分词编码字典
	 */
	private OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<>();

	/**
	 * 分词DF词频
	 */
	private OpenIntLongHashMap frequency = new OpenIntLongHashMap();

	/**
	 * tfidf计算工具
	 */
	private TFIDF tfidf = new TFIDF();

	/**
	 * 1 初始化分词工具 2 加载分词词典
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// 加载分词编码字典
		Path dictionaryFile = new Path("hdfs://172.19.10.33:8020/tmp/bayes/output/dictionary.file-0");
		for (Pair<Writable, IntWritable> record : new SequenceFileIterable<Writable, IntWritable>(
				dictionaryFile, true, conf)) {
			dictionary.put(record.getFirst().toString(),
					record.getSecond().get());
		}
		// 加载DF词频信息
		Path frequencyFile = new Path("hdfs://172.19.10.33:8020/tmp/bayes/output/frequency.file-0");
		for (Pair<IntWritable, LongWritable> record : new SequenceFileIterable<IntWritable, LongWritable>(
				frequencyFile, true, conf)) {
			frequency.put(record.getFirst().get(), record.getSecond().get());
		}
	}

	@Override
	protected void map(Text key, StringTuple value, Context context)
			throws IOException, InterruptedException {
		// 抓取、过滤、ik
		String webUrl = key.toString();
		List<String> termList = value.getEntries();

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
		Vector tfidfVector = new RandomAccessSparseVector(1, size);
		for (Vector.Element e : vector.nonZeroes()) {
			if (!frequency.containsKey(e.index())) {
				continue;
			}
			long df = frequency.get(e.index());
			if (df < 1) {
				df = 1;
			}
			
			// TF-IDF = TF * log(46248/df)
			tfidfVector.setQuick(e.index(), tfidf.calculate((int) e.get(), (int) df, (int) 1, (int) 46248));
		}

		// 名称向量，输出。
		tfidfVector = new NamedVector(tfidfVector, webUrl);
		VectorWritable vectorWritable = new VectorWritable(tfidfVector);
		context.write(new Text(webUrl), vectorWritable);
	}
	
}