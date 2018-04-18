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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.mahout.driver.map.Text2VectorMapper;

/**
 * 将文本转换为向量。
 * @author yhaitao
 *
 */
public class BayesText2Vector extends AbstractJob {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(BayesText2Vector.class);
	
	/**
	 * 文本转换向量驱动。
	 * @param args com.yhaitao.mahout.bayes.BayesText2Vector -i /tmp/bayes/urls/input -o /tmp/bayes/urls/vectors -b /tmp/bayes/output -ext /tmp/lib/mahout-demo
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
		addOption("base", "b", "The output of the seq2sparse.", true);
		addOption("ext", "ext", "The path of the ext jars.", true);
		if(parseArguments(args) == null) {
			return 1;
		}
		
		// 获取参数
		Path input = getInputPath();
		Path output = getOutputPath();
		String basePath = getOption("base");
		String extPath = getOption("ext");
		
	    // 任务创建
	    long start = System.currentTimeMillis();
	    String jobName = "BayesText2Vector-" + start;
	    Configuration conf = getConf();
	    Job job = Job.getInstance(conf, jobName);
	    job.setJarByClass(BayesText2Vector.class);
	    
	    // 资源文件加载
	    String dicPath = basePath + "/" + DictionaryVectorizer.DICTIONARY_FILE + "0";
	    String freqPath = basePath + "/" + TFIDFConverter.FREQUENCY_FILE + "0";
	    URI[] dicUris = getExt(conf, dicPath);
	    URI[] freqUris = getExt(conf, freqPath);
	    URI[] extUris = getExt(conf, extPath);
		String dicfiles = StringUtils.uriToString(dicUris);
		String freqfiles = StringUtils.uriToString(freqUris);
		String extfiles = StringUtils.uriToString(extUris);
		conf.set(MRJobConfig.CACHE_FILES, dicfiles);
		conf.set(MRJobConfig.CACHE_FILES, freqfiles);
		conf.set(MRJobConfig.CACHE_FILES, extfiles);
		job.setNumReduceTasks(1);
	    
	    // Map任务 
	    job.setMapperClass(Text2VectorMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(VectorWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VectorWritable.class);
	    
	    // 输入输出指定
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    SequenceFileInputFormat.setInputPaths(job, input);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setOutputPath(job, output);
	    
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
}
