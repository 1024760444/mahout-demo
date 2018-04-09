package com.yhaitao.mahout.bayes;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import com.yhaitao.mahout.crawler.bean.ChinazWeb;
import com.yhaitao.mahout.utils.ParamsUtils;

public class ChinazBayesClassifier {
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
		Map<String, List<ChinazWeb>> classWebMap = readOriginalFile(conf);
		
		/**
		 * 根据分类名称序列化数据到HDFS
		 */
		writeToSequenceFile(conf, in, classWebMap);
		
		/**
		 * 根据IK分词生成特征向量
		 * mahout seq2sparse 
		 * -i /tmp/bayes/input 
		 * -o /tmp/bayes/output 
		 * -lnorm -nv -ow -wt TFIDF 
		 * -a org.wltea.analyzer.lucene.IKAnalyzer
		 */
		sequenceToSparse(conf, in, out);
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
	private static void writeToSequenceFile(Configuration conf, String in, Map<String, List<ChinazWeb>> classWebMap) throws IOException {
		// SequenceFile写入工具
		Path path = new Path(in + "/part-m-00000");
		SequenceFile.Writer writer = SequenceFile.createWriter(
				conf, 
				new SequenceFile.Writer.Option[]{
						SequenceFile.Writer.file(path),
						Writer.keyClass(Text.class),
						Writer.valueClass(Text.class)
				});
		
		Iterator<Entry<String, List<ChinazWeb>>> iterator = classWebMap.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, List<ChinazWeb>> next = iterator.next();
			String key = next.getKey();
			List<ChinazWeb> value = next.getValue();
			for(ChinazWeb webInfo : value) {
				writer.append(
						new Text("/" + key + "/" + webInfo.getDomain()), 
						new Text(webInfo.getName() + " " + webInfo.getDesc()));
			}
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
	private static Map<String, List<ChinazWeb>> readOriginalFile(Configuration conf) throws ClassNotFoundException, SQLException {
		// 本地数据读取
		DBConfiguration dbConf = new DBConfiguration(conf);
		Connection connection = dbConf.getConnection();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select domain, class_name, web_name, web_desc from chinaz_web_info");
		
		// 分别写入
		Map<String, List<ChinazWeb>> classWebMap = new HashMap<String, List<ChinazWeb>>();
		Map<String, String> classInfo = new HashMap<String, String>();
		if(resultSet != null) {
			int classIndex = 0;
			while(resultSet.next()) {
				String domain = resultSet.getString("domain");
				String class_name = resultSet.getString("class_name");
				String web_name = resultSet.getString("web_name");
				String web_desc = resultSet.getString("web_desc");
				ChinazWeb chinazWeb = new ChinazWeb(web_name, web_desc, domain, class_name);
				
				// 分类编码: key -> class_name, value -> class_id
				String class_id = null;
				if(classInfo.containsKey(class_name)) {
					class_id = classInfo.get(class_name);
				} else {
					class_id = "BayesClassId_" + classIndex;
					classInfo.put(class_name, class_id);
					classIndex++;
				}
				
				// 数据分类
				if(class_id != null) {
					if(classWebMap.containsKey(class_id)) {
						List<ChinazWeb> list = classWebMap.get(class_id);
						list.add(chinazWeb);
					} else {
						List<ChinazWeb> list = new ArrayList<ChinazWeb>();
						list.add(chinazWeb);
						classWebMap.put(class_id, list);
					}
				}
			}
		}
		statement.close();
		connection.close();
		
		// write to class info
		writeClassInfo(conf, classInfo);
		return classWebMap;
	}
	
	/**
	 * 分类编码，写回mysql。
	 * @param conf
	 * @param classInfo key -> class_name, value -> class_id
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private static void writeClassInfo(Configuration conf, Map<String, String> classInfo) throws ClassNotFoundException, SQLException {
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
