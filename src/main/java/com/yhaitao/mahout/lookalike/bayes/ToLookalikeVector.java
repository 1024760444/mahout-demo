package com.yhaitao.mahout.lookalike.bayes;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.mahout.lookalike.bayes.map.ToLookalikeMapper;

/**
 * 将文本转换为向量。
 * @author yhaitao
 *
 */
public class ToLookalikeVector extends AbstractJob {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(ToLookalikeVector.class);
	
	/**
	 * 文本转换向量驱动。
	 * @param args com.yhaitao.mahout.bayes.BayesText2Vector -i /tmp/bayes/urls/input -o /tmp/bayes/urls/vectors -b /tmp/bayes/output -ext /tmp/lib/mahout-demo
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ToLookalikeVector(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// 解析必须参数
		addInputOption();
		addOutputOption();
		addOption("ext", "ext", "The path of the ext jars.", true);
		if(parseArguments(args) == null) {
			return 1;
		}
		
		// 获取参数
		Path input = getInputPath();
		Path output = getOutputPath();
		String extPath = getOption("ext");
		
	    // 任务创建
	    long start = System.currentTimeMillis();
	    String jobName = "ToLookalikeVector-" + start;
	    Configuration conf = getConf();
	    Job job = Job.getInstance(conf, jobName);
	    job.setJarByClass(ToLookalikeVector.class);
	    
	    // 资源文件加载
	    URI[] extUris = getExt(conf, extPath);
		String extfiles = StringUtils.uriToString(extUris);
		conf.set(MRJobConfig.CACHE_FILES, extfiles);
		job.setNumReduceTasks(1);
	    
	    // Map任务 
	    job.setMapperClass(ToLookalikeMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
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
