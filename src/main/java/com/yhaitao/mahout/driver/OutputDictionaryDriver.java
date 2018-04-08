package com.yhaitao.mahout.driver;

import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.mahout.driver.bean.ChinazDictionary;
import com.yhaitao.mahout.driver.map.ChinazDictionaryMapper;
import com.yhaitao.mahout.driver.output.ReplaceDBOutputFormat;

/**
 * 词典导出，分词编码。
 * @author yhaitao
 *
 */
public class OutputDictionaryDriver extends AbstractJob {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(OutputDictionaryDriver.class);
	
	/**
	 * 表名
	 */
	private final static String TABLE_NAME = "chinaz_dictionary";
	private final static String[] FIELDNAMES = new String[]{"dic_id", "dic_name"};
	
	/**
	 * hadoop jar mahout-demo.jar com.yhaitao.mahout.driver.OutputDictionaryDriver -i /tmp/kmeans/output/dictionary.file-0 -url jdbc:mysql://172.19.10.33:3306/kmeans -uname root -passwd 123456
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new OutputDictionaryDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOptionForMysql();
		if (parseArguments(args) == null) {
			return -1;
		}
		Path input = getInputPath();
		String url = getOption("url");
		String uname = getOption("uname");
		String passwd = getOption("passwd");
		Configuration conf = getConf();
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", url, uname, passwd);
		
	    // 任务创建
	    long start = System.currentTimeMillis();
	    String jobName = "OutputDictionaryDriver-" + start;
	    Job countJob = Job.getInstance(conf, jobName);
	    countJob.setJarByClass(OutputDictionaryDriver.class);
	    countJob.setNumReduceTasks(0);
	    
	    // Map任务：词典
	    countJob.setMapperClass(ChinazDictionaryMapper.class);
		countJob.setMapOutputKeyClass(ChinazDictionary.class);
		countJob.setMapOutputValueClass(NullWritable.class);
	    
	    // 输入输出指定
	    countJob.setInputFormatClass(SequenceFileInputFormat.class);
	    SequenceFileInputFormat.setInputPaths(countJob, input);
	    countJob.setOutputFormatClass(ReplaceDBOutputFormat.class);
	    ReplaceDBOutputFormat.setOutput(countJob, TABLE_NAME, FIELDNAMES);

	    // 提交并等待任务
	    LOGGER.info("Job : {} submit for path : {}.", jobName, TABLE_NAME);
	    boolean hasSuccess = countJob.waitForCompletion(true);
	    long cost = System.currentTimeMillis() - start;
	    LOGGER.info("Job : {} already completed : {}, const times : {} ms.", jobName, hasSuccess, cost);
	    return hasSuccess ? 0 : 1;
	}
	
	/**
	 * 针对Mysql数据库信息。
	 */
	private void addOutputOptionForMysql() {
		// 数据库的连接信息
		addOption(new DefaultOptionBuilder()
				.withLongName("url")
				.withRequired(true)
				.withShortName("url")
				.withArgument(new ArgumentBuilder().withName("url").create())
				.withDescription("mysql url. e.g. jdbc:mysql://172.19.10.33:3306/kmeans")
				.create());
		addOption(new DefaultOptionBuilder()
				.withLongName("uname")
				.withRequired(true)
				.withShortName("uname")
				.withArgument(new ArgumentBuilder().withName("uname").create())
				.withDescription("mysql uname. e.g. root")
				.create());
		addOption(new DefaultOptionBuilder()
				.withLongName("passwd")
				.withRequired(true)
				.withShortName("passwd")
				.withArgument(new ArgumentBuilder().withName("passwd").create())
				.withDescription("mysql passwd. e.g. 123456")
				.create());
	}
}
