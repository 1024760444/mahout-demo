package com.yhaitao.mahout.kmeans;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import com.google.gson.Gson;
import com.yhaitao.mahout.driver.ClusterResultParseDriver;
import com.yhaitao.mahout.driver.OutputDictionaryDriver;

/**
 * 将本地的站长之家数据文件以SequenceFile方式上传到HDFS。
 * @author yhaitao
 *
 */
public class ChinazKMeansCluster {
	public final static Gson GSON = new Gson();
	
	/**
	 * 任务入口。
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 参数解析，获取参数
		CommandLine commands = getCommandLine(args);
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
		URI[] uriArray = getExtJars(conf, extJars);
		String sfiles = StringUtils.uriToString(uriArray);
		conf.set(MRJobConfig.CACHE_FILES, sfiles);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", url, uname, passwd);
		DBConfiguration dbConf = new DBConfiguration(conf);
		
		/**
		 * 原始数据
		 * key ： 网站域名
		 * value ： 网站文本描述
		 */
		Map<String, String> map = readOriginalFile(dbConf);
		
		// 数据写入HDFS
		writeToSequenceFile(conf, in, map);
		
		/**
		 * 根据IK分词生成特征向量
		 * mahout seq2sparse 
		 * -i /tmp/kmeans/input 
		 * -o /tmp/kmeans/output 
		 * -lnorm -nv -ow -wt TFIDF 
		 * -a org.wltea.analyzer.lucene.IKAnalyzer
		 */
		sequenceToSparse(conf, in, out);
		
		/**
		 * kmeans聚类
		 * mahout kmeans 
		 * -i /tmp/kmeans/output/tfidf-vectors 
		 * -o /tmp/kmeans/cosine/output 
		 * -dm org.apache.mahout.common.distance.CosineDistanceMeasure 
		 * -c /tmp/kmeans/cosine/clusters 
		 * -k 12 -cd 0.5 -x 50 -ow -cl -xm mapreduce
		 */
		kmeansCluster(conf, out);
		
		/**
		 * 聚类结果解析
		 */
		kmeansClusterParse(conf, out, url, uname, passwd);
		
		// 词典导出
		kmeansDictionaryOutput(conf, out, url, uname, passwd);
	}
	
	/**
	 * 词典导出
	 * @param conf
	 * @param out
	 * @param url
	 * @param uname
	 * @param passwd
	 * @throws Exception 
	 */
	private static void kmeansDictionaryOutput(Configuration conf, String out,
			String url, String uname, String passwd) throws Exception {
		String[] args = {
				"-i", out + "/dictionary.file-0",
				"-url", "jdbc:mysql://172.19.10.33:3306/kmeans",
				"-uname", "root",
				"-passwd", "123456",
		};
		ToolRunner.run(conf, new OutputDictionaryDriver(), args);
	}

	/**
	 * 解析聚类结果，导入Mysql中。
	 * @param conf 环境配置
	 * @param url 数据库连接
	 * @param uname 数据库名称
	 * @param passwd 数据库密码
	 * @throws Exception 
	 */
	private static void kmeansClusterParse(Configuration conf, String input, 
			String url,	String uname, String passwd) throws Exception {
		String[] args = {
				"-i", input + "/output/clusteredPoints",
				"-url", "jdbc:mysql://172.19.10.33:3306/kmeans",
				"-uname", "root",
				"-passwd", "123456",
		};
		ToolRunner.run(conf, new ClusterResultParseDriver(), args);
	}

	/**
	 * 提交，进行kmeans聚类
	 * @param conf 环境配置
	 * @param out 特征向量目录
	 * @param cosine 聚类结果目录
	 * @throws Exception 
	 */
	private static void kmeansCluster(Configuration conf, String out) throws Exception {
		String[] args = {
				"-i", out + "/tfidf-vectors",
				"-o", out + "/output",
				"-c", out + "/clusters",
				"-dm", "org.apache.mahout.common.distance.CosineDistanceMeasure",
				"-x", "10",
				"-k", "12",
				"-cl",
				"-ow",
				"-xm", "mapreduce"
		};
		ToolRunner.run(conf, new KMeansDriver(), args);
	}
	
	/**
	 * 生成向量
	 * @param in
	 * @param out
	 * @throws Exception 
	 */
	public static void sequenceToSparse(Configuration conf, String in, String out) throws Exception {
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
	 * 读取本地文件数据
	 * @param local
	 * @return
	 * @throws IOException
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private static Map<String, String> readOriginalFile(DBConfiguration dbConf) throws IOException, ClassNotFoundException, SQLException {
		Map<String, String> mapResult = new HashMap<String, String>();
		Connection connection = dbConf.getConnection();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select domain, web_name, web_desc from chinaz_web_info");
		if(resultSet != null) {
			while(resultSet.next()) {
				String domain = resultSet.getString("domain");
				String web_name = resultSet.getString("web_name");
				String web_desc = resultSet.getString("web_desc");
				if(null != domain && !"".equals(domain)) {
					mapResult.put(domain, web_name + " " + web_desc);
				}
			}
		}
		return mapResult;
	}
	
	/**
	 * 获取第三方jar包的路径。
	 * @param conf 集群配置
	 * @param extJarsPath 第三方Jar包路径
	 * @return 路径
	 * @throws IOException
	 */
	private static URI[] getExtJars(Configuration conf, String extJarsPath) throws IOException {
		// 读取hdfs文件，获取jar列表
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(extJarsPath), false);
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
	 * 数据上传HDFS。
	 * @param original
	 * @param in
	 * @throws IOException 
	 */
	private static void writeToSequenceFile(Configuration conf, String in, Map<String, String> map) throws IOException {
		// SequenceFile写入工具
		Path path = new Path(in + "/part-m-00000");
		SequenceFile.Writer writer = SequenceFile.createWriter(
				conf, 
				new SequenceFile.Writer.Option[]{
						SequenceFile.Writer.file(path),
						Writer.keyClass(Text.class),
						Writer.valueClass(Text.class)
				});
		
		// 写入数据
		for(String key : map.keySet()) {
			String value = map.get(key);
			writer.append(new Text(key), new Text(value));
		}
		writer.close();
	}
	
	/**
	 * 入参校验
	 * @param args 入参
	 * @throws ParseException 
	 */
	private static CommandLine getCommandLine(String[] args) throws ParseException {
		Options options = buildOptions();
		BasicParser parser = new BasicParser();
		CommandLine commands = parser.parse(options, args);
		if(!commands.hasOption("in")
				|| !commands.hasOption("out")
				|| !commands.hasOption("url")
				|| !commands.hasOption("uname")
				|| !commands.hasOption("passwd")
				|| !commands.hasOption("extJars")) {
			printUsage(options);
			return null;
		} else {
			return commands;
		}
	}
	
	/**
	 * 输入信息提示。
	 * @return 需要输入数据的说明
	 */
	private static Options buildOptions() {
		Options options = new Options();
		options.addOption("in", true, "[required] HDFS path for kmeans input");
		options.addOption("out", true, "[required] HDFS path for kmeans out");
		options.addOption("url", true, "[required] mysql url. e.g. jdbc:mysql://172.19.10.33:3306/kmeans");
		options.addOption("uname", true, "[required] mysql uname. e.g. root");
		options.addOption("passwd", true, "[required] mysql passwd. e.g. 123456");
		options.addOption("extJars", true, "[required] HDFS path ext jars");
		return options;
	}
	
	/**
	 * 打印输入信息。
	 * @param options 打印输入帮助信息。
	 */
	public static void printUsage(Options options) {
		HelpFormatter help = new HelpFormatter();
		help.printHelp("Job of KMeansCluster need Params : ", options);
	}
}