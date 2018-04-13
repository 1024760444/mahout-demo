package com.yhaitao.mahout.bayes;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.test.TestNaiveBayesDriver;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.utils.SplitInput;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import com.yhaitao.mahout.crawler.bean.ChinazWeb;
import com.yhaitao.mahout.utils.ParamsUtils;

public class BayesClassifier {
	/**
	 * 任务入口。
	 * hadoop jar mahout-demo.jar com.yhaitao.mahout.bayes.ChinazBayesClassifier 
	 * -in /tmp/bayes/input 
	 * -out /tmp/bayes/output 
	 * -extJars /tmp/lib/mahout-demo 
	 * -url jdbc:mysql://172.19.10.33:3306/kmeans 
	 * -uname root 
	 * -passwd 123456
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 参数解析，获取参数
		CommandLine commands = ParamsUtils.getCommandLine(args);
		if(commands == null) {
			return ;
		}
		String in = commands.getOptionValue("in");
		String out = commands.getOptionValue("out");
		String extJars = commands.getOptionValue("extJars");
		
		String url = commands.getOptionValue("url");
		String uname = commands.getOptionValue("uname");
		String passwd = commands.getOptionValue("passwd");
		
		// 参数设置
		Configuration conf = new Configuration();
		URI[] uriArray = ParamsUtils.getExtJars(conf, extJars);
		String sfiles = StringUtils.uriToString(uriArray);
		conf.set(MRJobConfig.CACHE_FILES, sfiles);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", url, uname, passwd);
		
		/**
		 * 读取数据库中已有的样本数据。
		 */
		List<ChinazWeb> webInfoList = readOriginalFile(conf);
		
		/**
		 * 根据分类名称序列化数据到HDFS
		 */
		writeToSequenceFile(conf, in, webInfoList);
		
		/**
		 * 根据IK分词生成特征向量
		 * mahout seq2sparse 
		 * -i /tmp/bayes/input 
		 * -o /tmp/bayes/output 
		 * -lnorm -nv -ow -wt TFIDF 
		 * -a org.wltea.analyzer.lucene.IKAnalyzer
		 */
		sequenceToSparse(conf, in, out);
		
		/**
		 * 向量拆分：训练集与测试集
		 * mahout split 
		 * --input /tmp/bayes/output/tfidf-vectors 
		 * --trainingOutput /tmp/bayes/output/trainingOutput 
		 * --testOutput /tmp/bayes/output/testOutput 
		 * --randomSelectionPct 20 --overwrite --sequenceFiles --method sequential
		 */
		splitSparses(conf, out);
		
		/**
		 * 训练
		 * mahout trainnb 
		 * -i /tmp/bayes/output/trainingOutput 
		 * -o /tmp/bayes/output/model 
		 * -li /tmp/bayes/output/labelindex 
		 * -ow -c -el 
		 */
		trainnb(conf, out);
		
		/**
		 * 测试
		 * mahout testnb 
		 * -i /tmp/bayes/output/testOutput 
		 * -m /tmp/bayes/output/model 
		 * -l /tmp/bayes/output/labelindex 
		 * -o /tmp/bayes/output/test_20180409_1 
		 * -c -ow 
		 */
		testnb(conf, out);
	}
	
	/**
	 * 测试测试集
	 * @param conf 环境配置
	 * @param out 输出目录
	 * @throws Exception 
	 */
	private static void testnb(Configuration conf, String out) throws Exception {
		String[] args = {
				"-i", out + "/testOutput",
				"-m", out + "/model",
				"-l", out + "/labelindex", 
				"-o", out + "/testing-result", 
				"-ow", 
				"-c"
		};
		ToolRunner.run(conf, new TestNaiveBayesDriver(), args);
	}

	/**
	 * 训练训练集
	 * @param conf 环境配置
	 * @param out 输出目录
	 * @throws Exception 
	 */
	private static void trainnb(Configuration conf, String out) throws Exception {
		String[] args = {
				"-i", out + "/trainingOutput",
				"-o", out + "/model",
				"-li", out + "/labelindex", 
				"-el", 
				"-ow", 
				"-c"
		};
		ToolRunner.run(conf, new TrainNaiveBayesJob(), args);
	}

	/**
	 * 数据拆分为训练集与测试集
	 * @param conf 环境配置
	 * @param out 输出目录
	 * @throws Exception 
	 */
	private static void splitSparses(Configuration conf, String out) throws Exception {
		String[] args = {
				"--input", out + "/tfidf-vectors",
				"--trainingOutput", out + "/trainingOutput",
				"--testOutput", out + "/testOutput", 
				"--randomSelectionPct", "20", 
				"--overwrite", 
				"--sequenceFiles", 
				"--method", "sequential" 
		};
		ToolRunner.run(conf, new SplitInput(), args);
	}

	/**
	 * 生成向量
	 * @param in
	 * @param out
	 * @throws Exception 
	 */
	private static void sequenceToSparse(Configuration conf, String in, String out) throws Exception {
		String[] args = {
				"-i", in,
				"-o", out,
				"-lnorm", // 归一化
				"-nv", 
				"-ow", // 已有输出，覆盖
				"-wt", "tfidf", // 计算权重
				"-a", "org.wltea.analyzer.lucene.IKAnalyzer" // IK分词器
		};
		ToolRunner.run(conf, new SparseVectorsFromSequenceFiles(), args);
	}
	
	/**
	 * 序列化SequenceFile并上传数据到HDFS。
	 * @param conf 配置信息
	 * @param in 序列化文件保存的HDFS目录
	 * @param classWebMap 分类的样本数据
	 * @throws IOException
	 */
	private static void writeToSequenceFile(Configuration conf, String in, List<ChinazWeb> webInfoList) throws IOException {
		// SequenceFile写入工具
		Path path = new Path(in + "/part-m-00000");
		SequenceFile.Writer writer = SequenceFile.createWriter(
				conf, 
				new SequenceFile.Writer.Option[]{
						SequenceFile.Writer.file(path),
						Writer.keyClass(Text.class),
						Writer.valueClass(Text.class)
				});
		
		for (ChinazWeb webInfo : webInfoList) {
			String class_name = webInfo.getClass_name();
			String domain = webInfo.getDomain();
			String name = webInfo.getName();
			String desc = webInfo.getDesc();
			writer.append(
					new Text("/" + class_name + "/" + domain), 
					new Text(name + " " + desc));
		}
		writer.close();
	}
	
	/**
	 * 读取数据样本信息到内存。
	 * @param conf 本地配置信息
	 * @return 数据样本信息列表
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private static List<ChinazWeb> readOriginalFile(Configuration conf) throws ClassNotFoundException, SQLException {
		// 本地数据读取
		DBConfiguration dbConf = new DBConfiguration(conf);
		Connection connection = dbConf.getConnection();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select domain, class_name, web_name, web_desc from chinaz_web_info");
		
		// 分别写入
		List<ChinazWeb> classWebList = new ArrayList<ChinazWeb>();
		if(resultSet != null) {
			while(resultSet.next()) {
				String domain = resultSet.getString("domain");
				String class_name = resultSet.getString("class_name");
				String web_name = resultSet.getString("web_name");
				String web_desc = resultSet.getString("web_desc");
				if(null == class_name 
						|| "".equals(class_name) 
						|| "综合其他".equals(class_name)) {
					continue;
				}
				classWebList.add(new ChinazWeb(web_name, web_desc, domain, class_name));
			}
		}
		statement.close();
		connection.close();
		return classWebList;
	}
	
	/**
	 * 分类编码，写回mysql。
	 * @param conf
	 * @param classInfo key -> class_name, value -> class_id
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void writeClassInfo(Configuration conf, Map<String, String> classInfo) throws ClassNotFoundException, SQLException {
		// 本地数据读取
		DBConfiguration dbConf = new DBConfiguration(conf);
		Connection connection = dbConf.getConnection();
		connection.setAutoCommit(false);
		PreparedStatement statement = connection.prepareStatement("replace into class_info (class_id, class_name) values (?, ?)");
		Iterator<Entry<String, String>> iterator = classInfo.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, String> next = iterator.next();
			String key = next.getKey();
			String value = next.getValue();
			statement.setString(1, value);
			statement.setString(2, key);
			statement.addBatch();
		}
		statement.executeBatch();
		connection.commit();
		close(connection, statement);
	}
	
	/**
	 * 关闭相关连接。
	 * 
	 * @param connection
	 * @param statement
	 * @throws SQLException
	 */
	private static void close(Connection connection, PreparedStatement statement)
			throws SQLException {
		if (connection != null) {
			connection.close();
		}
		if (statement != null) {
			statement.close();
		}
	}
}
