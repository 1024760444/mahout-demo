package com.yhaitao.mahout.bayes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.StringTuple;

import com.yhaitao.mahout.crawler.PageAnalysis;
import com.yhaitao.mahout.crawler.PageCrawler;

/**
 * 输入网页链接， 转换为特征向量。
 * @author yhaitao
 *
 */
public class UrlToVector extends AbstractJob {
	/**
	 * 特征向量转换。
	 * hadoop jar mahout-demo.jar com.yhaitao.mahout.bayes.UrlToVector -l /root/demo/mahout-demo/webIK.txt -i /tmp/bayes/urls/input
	 * @param args -i /tmp/bayes/urls/input -o /tmp/bayes/urls/output
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new UrlToVector(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// 参数设置
		addInputOption();
		addOutputOption();
		if(null == parseArguments(args)) {
			return 1;
		}
		
		// 获取参数
//		Path input = getInputPath();
//		Path output = getOutputPath();
//		Configuration conf = getConf();
		return 0;
	}
	
	/**
	 * 序列化文件。
	 * @param args
	 * @throws IOException
	 */
	public void tokenized(String[] args) throws IOException {
		// crawler();
		addOption("local", "l", "", true);
		addInputOption();
		if(null == parseArguments(args)) {
			return ;
		}
		
		// 获取参数
		String local = getOption("local");
		Configuration conf = getConf();
		String url = "http://uland.taobao.com/sem/tbsearch?refpid=mm_26632258_3504122_32538762&keyword=wow&clk1=4436d264cd5d2ba3a96e4dc41f839b1c&upsid=4436d264cd5d2ba3a96e4dc41f839b1c";
		
		// 写数据
		Path path = new Path("/tmp/bayes/urls/input/part-m-00000");
		SequenceFile.Writer writer = SequenceFile.createWriter(
				conf, 
				new SequenceFile.Writer.Option[] {
						SequenceFile.Writer.file(path),
						Writer.keyClass(Text.class),
						Writer.valueClass(StringTuple.class)
				});
		
		// 
		List<String> readLines = FileUtils.readLines(new File(local), "UTF-8");
		if(readLines.size() > 0) {
			String line = readLines.get(0);
			String[] terms = line.split(" ");
			StringTuple document = new StringTuple();
			for(String term : terms) {
				document.add(term);
			}
			writer.append(new Text(url), document);
		}
		
		/**
		BufferedReader bReader = new BufferedReader(new FileReader(new File(local)));
		String line = null;
		while((line = bReader.readLine()) != null) {
			String[] split = line.split("\\|");
			if(split != null 
					&& split.length > 2 
					&& StringUtils.isNotBlank(split[0])) {
				String[] terms = split[1].split(" ");
				StringTuple document = new StringTuple();
				for(String term : terms) {
					document.add(term);
				}
				writer.append(new Text(split[0]), document);
				System.err.println("line : " + split[0]);
			}
		}
		bReader.close();
		**/
	}
	
	/**
	 * 抓取网页，分词。
	 * @throws Exception 
	 */
	public void crawler() throws Exception {
		// 获取参数
		String local = "C:/Users/admin/Desktop/urls.txt";
		
		// 抓取、过滤、分词
		PageAnalysis pageAnalysis = new PageAnalysis();
		BufferedReader reader = new BufferedReader(new FileReader(new File(local)));
		String line = null;
		File webPathFile = new File("C:/Users/admin/Desktop/webIK.txt");
		while((line = reader.readLine()) != null) {
			String pageContext = PageCrawler.crawler(line);
			List<String> termList = pageAnalysis.analyzer(line, pageContext);
			FileUtils.write(webPathFile, (line + "|" + chg(termList) + "\n"), "UTF-8", true);
		}
		reader.close();
	}
	
	/**
	 * 将分词列表拼接为字符串，以空格分割。
	 * @param termList 分词列表
	 * @return 分词字符串
	 */
	public static String chg(List<String> termList) {
		StringBuffer sb = new StringBuffer();
		for(String term : termList) {
			sb.append(term).append(" ");
		}
		return sb.toString();
	}
	
	public static List<Path> getExt(Configuration conf, String extPath) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(extPath), false);
		List<Path> pathList = new ArrayList<Path>();
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			pathList.add(next.getPath());
		}
		fileSystem.close();
		return pathList;
	}
}
