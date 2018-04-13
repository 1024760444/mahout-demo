package com.yhaitao.mahout.bayes;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.TFIDF;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.mahout.crawler.PageAnalysis;
import com.yhaitao.mahout.crawler.PageCrawler;

/**
 * 将文本转换为向量。
 * @author yhaitao
 *
 */
@SuppressWarnings("deprecation")
public class BayesText2Vector extends AbstractJob {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(BayesText2Vector.class);
	
	/**
	 * 文本转换向量驱动。
	 * @param args -i /tmp/bayes/urls/input -o /tmp/bayes/urls/vectors -s /tmp/bayes/output -ext /tmp/lib/mahout-demo
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new BayesText2Vector(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// 解析必须参数
		addInputOption();
		addOutputOption();
		addOption("seq2sparse", "s", "The output of the seq2sparse.", true);
		addOption("ext", "ext", "The path of the ext jars.", true);
		if(parseArguments(args) == null) {
			return 1;
		}
		
		// 获取参数
		Path input = getInputPath();
		Path output = getOutputPath();
		String basePath = getOption("s");
		String extPath = getOption("ext");
		
	    // 任务创建
	    long start = System.currentTimeMillis();
	    String jobName = "BayesText2Vector-" + start;
	    Configuration conf = getConf();
	    Job job = Job.getInstance(conf, jobName);
	    job.setJarByClass(BayesText2Vector.class);
	    
	    // 资源文件加载
	    basePath = basePath + "/" + DictionaryVectorizer.DICTIONARY_FILE + "*";
	    URI[] baseUris = getExt(conf, basePath);
	    URI[] extUris = getExt(conf, extPath);
		String basefiles = StringUtils.uriToString(baseUris);
		String extfiles = StringUtils.uriToString(extUris);
		conf.set(MRJobConfig.CACHE_FILES, basefiles);
		conf.set(MRJobConfig.CACHE_FILES, extfiles);
	    
	    // Map任务 
	    job.setMapperClass(Text2VectorMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(VectorWritable.class);
	    
	    // 输入输出指定
	    job.setInputFormatClass(CombineTextInputFormat.class);
	    CombineTextInputFormat.setInputPaths(job, input);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    // 提交并等待任务
	    LOGGER.info("Job : {} submit for path : {}.", jobName, output.getName());
	    boolean hasSuccess = job.waitForCompletion(true);
	    long cost = System.currentTimeMillis() - start;
	    LOGGER.info("Job : {} already completed : {}, const times : {} ms.", jobName, hasSuccess, cost);
	    return hasSuccess ? 0 : 1;
	}
	
	/**
	 * 获取第三方资源路径信息。
	 * @param conf HDFS环境配置
	 * @param extPath 第三方资源的目录
	 * @return 第三方资源的url信息。
	 * @throws IOException
	 */
	private static URI[] getExt(Configuration conf, String extPath) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(extPath), false);
		List<URI> uriList = new ArrayList<URI>();
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			uriList.add(next.getPath().toUri());
		}
		fileSystem.close();
		URI[] array = new URI[uriList.size()];
		return uriList.toArray(array);
	}
	
	/**
	 * 构建向量。
	 * 抓取、过滤网页，获取文本
	 * IK分词获取，词频统计TF
	 * 单词编码
	 * 根据DF计算向量（tfidf）
	 * @author admin
	 *
	 */
	public class Text2VectorMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {
		/**
		 * 分词工具
		 */
		private PageAnalysis pageAnalysis;
		
		/**
		 * 分词编码字典
		 */
		private final OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<>();
		
		/**
		 * 分词DF词频
		 */
		private final OpenIntLongHashMap frequency = new OpenIntLongHashMap();
		
		/**
		 * tfidf计算工具
		 */
		private final TFIDF tfidf = new TFIDF();
		
		/**
		 * 1 初始化分词工具
		 * 2 加载分词词典
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.pageAnalysis = new PageAnalysis();
			
			// 加载分词编码字典
			URI[] localFiles = DistributedCache.getCacheFiles(conf);
		    Path dictionaryFile = HadoopUtil.findInCacheByPartOfFilename(DictionaryVectorizer.DICTIONARY_FILE, localFiles);
			for (Pair<Writable, IntWritable> record : 
				new SequenceFileIterable<Writable, IntWritable>(dictionaryFile, true, conf)) {
				dictionary.put(record.getFirst().toString(), record.getSecond().get());
			}
			
			// 加载DF词频信息
			Path frequencyFile = HadoopUtil.findInCacheByPartOfFilename(TFIDFConverter.FREQUENCY_FILE, localFiles);
			for (Pair<IntWritable, LongWritable> record : 
				new SequenceFileIterable<IntWritable, LongWritable>(frequencyFile, true, conf)) {
				frequency.put(record.getFirst().get(),record.getSecond().get());
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			// 抓取、过滤、ik
			String webUrl = value.toString();
			List<String> termList = null;
			try {
				String pageContext = PageCrawler.crawler(webUrl);
				termList = this.pageAnalysis.analyzer(webUrl, pageContext);
			} catch (Exception e) {
				return ;
			}
			
			// 组建TF向量
			int size = dictionary.size();
			Vector vector = new RandomAccessSparseVector(size, size);
			for(String termName : termList) {
				if(null != termName 
						&& !"".equals(termName) 
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
				tfidfVector.setQuick(e.index(), tfidf.calculate((int) e.get(), (int) df, (int) 1, (int) 1));
			}
			
			// 名称向量，输出。
			tfidfVector = new NamedVector(tfidfVector, webUrl);
			VectorWritable vectorWritable = new VectorWritable(tfidfVector);
			context.write(new Text(webUrl), vectorWritable);
		}
	}
}
