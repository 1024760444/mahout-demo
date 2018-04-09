package com.yhaitao.mahout.utils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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

import com.google.gson.Gson;

/**
 * 参数工具类。
 * @author yhaitao
 *
 */
public class ParamsUtils {
	public final static Gson GSON = new Gson();
	
	/**
	 * 入参校验
	 * @param args 入参
	 * @throws ParseException 
	 */
	public static CommandLine getCommandLine(String[] args) throws ParseException {
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
	 * 获取第三方jar包的路径。
	 * @param conf 集群配置
	 * @param extJarsPath 第三方Jar包路径
	 * @return 路径
	 * @throws IOException
	 */
	public static URI[] getExtJars(Configuration conf, String extJarsPath) throws IOException {
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
	private static void printUsage(Options options) {
		HelpFormatter help = new HelpFormatter();
		help.printHelp("Job need Params : ", options);
	}
}
