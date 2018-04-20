package com.yhaitao.mahout.bayes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.StringTuple;

import com.yhaitao.mahout.crawler.PageAnalysis;
import com.yhaitao.mahout.crawler.PageCrawler;

public class ToSequenceFile extends AbstractJob {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ToSequenceFile(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// 参数解析
		addOption("local", "l", "Local urls", true);
		addInputOption();
		if(parseArguments(args) == null) {
			return 1;
		}
		Configuration conf = getConf();
		
		// 参数获取
		Path input = getInputPath();
		String local = getOption("local");
		
		// 序列化数据
		Path path = new Path(input, "/part-m-00000");
		SequenceFile.Writer writer = SequenceFile.createWriter(
				conf,
				new SequenceFile.Writer.Option[] {
						SequenceFile.Writer.file(path),
						Writer.keyClass(Text.class),
						Writer.valueClass(StringTuple.class) });
		
		// 抓取、过滤、ik分词
		PageAnalysis analysis = new PageAnalysis();
		InputStreamReader reader = new InputStreamReader(new FileInputStream(local));
		BufferedReader br = new BufferedReader(reader);
		String line = null;
		while((line = br.readLine()) != null) {
			try {
				String pageContext = PageCrawler.crawler(line);
				List<String> termList = analysis.analyzer(line, pageContext);
				StringTuple document = new StringTuple();
				for(String term : termList) {
					document.add(term);
				}
				writer.append(new Text(line), document);
			} catch (Exception e) {
				System.err.println(line + ", " + e.getMessage());
			}
		}
		br.close();
		reader.close();
		return 0;
	}

}
