package com.yhaitao.mahout.lookalike.job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.mahout.driver.output.NoChkFileOutputFormat;
import com.yhaitao.mahout.lookalike.job.map.To11VectorMapper;
import com.yhaitao.mahout.lookalike.job.reduce.To11VectorReducer;
import com.yhaitao.mahout.utils.ParamsUtils;

/**
 * 根据用户信息，获取用户11维度特征向量
 * @author yhaitao
 *
 */
public class To11Vector extends AbstractJob {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(To11Vector.class);
	
	/**
	 * 用户特征数据解析。Coarse-grained
	 * @param args -i /user/hive/warehouse/lookalike.db/use_feature_all -o /tmp/lookalike/kmeans/output -ext /tmp/lib/mahout-demo
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new To11Vector(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// 参数设置
		addInputOption();
		addOutputOption();
		addOption("ext", "ext", "Ext resources path.", true);
		if(null == parseArguments(args)) {
			return 1;
		}
		Configuration conf = getConf();
		
		// 参数获取
		Path input = getInputPath();
		Path output = getOutputPath();
		String extPath = getOption("ext");
		
		// 第三方资源文件
		URI[] uriArray = ParamsUtils.getExtJars(conf, extPath);
		String sfiles = StringUtils.uriToString(uriArray);
		conf.set(MRJobConfig.CACHE_FILES, sfiles);
		conf.set("mapred.textoutputformat.separator", "\t");   // MR数据分隔符设置
		
		/**
		 * Map个数设计
		 * mapreduce.input.fileinputformat.split.minsize
		 * mapreduce.input.fileinputformat.split.maxsize
		 */
		conf.setLong("mapreduce.input.fileinputformat.split.minsize", 1073741824l);
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 1073741824l);
		
	    // 任务创建
	    long start = System.currentTimeMillis();
	    String jobName = "To11Vector-" + start;
	    Job job = Job.getInstance(conf, jobName);
	    job.setJarByClass(To11Vector.class);
		
	    // Map任务
	    job.setMapperClass(To11VectorMapper.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    // Reduce任务
	    job.setReducerClass(To11VectorReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(VectorWritable.class);
	    
	    // 输入输出指定
	    job.setInputFormatClass(CombineTextInputFormat.class);
	    FileInputFormat.setInputPaths(job, input);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    NoChkFileOutputFormat.setOutputPath(job, output);
	    
	    // 提交并等待任务
	    LOGGER.info("Job : {} submit for path : {}.", jobName, output);
	    boolean hasSuccess = job.waitForCompletion(true);
	    long cost = System.currentTimeMillis() - start;
	    LOGGER.info("Job : {} already completed : {}, const times : {} ms.", jobName, hasSuccess, cost);
	    return hasSuccess ? 0 : 1;
	}
}
