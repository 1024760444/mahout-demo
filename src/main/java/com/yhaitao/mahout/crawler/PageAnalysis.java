package com.yhaitao.mahout.crawler;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.StringTuple;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * IK分词
 * @author admin
 *
 */
public class PageAnalysis {
	/**
	 * 分词器
	 */
	private Analyzer analyzer;
	
	/**
	 * 构建分词器，使用IK分词。
	 */
	public PageAnalysis() {
		this.analyzer = new IKAnalyzer(true);
	}
	
	/**
	 * 对输入文本进行分词。
	 * @param key 文本标识
	 * @param context 输入文本
	 * @return 分词列表
	 * @throws IOException 
	 */
	public List<String> analyzer(String key, String context) throws IOException {
		// 构造分词迭代器
		TokenStream stream = this.analyzer.tokenStream(key, new StringReader(context));
		CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
		stream.reset();
		
		// 读取分词
		List<String> termList = new ArrayList<String>();
		while(stream.incrementToken()) {
			if(termAtt.length() > 0) {
				termList.add(new String(termAtt.buffer(), 0, termAtt.length()));
			}
		}
		return termList;
	}
	
	/**
	 * 对输入文本进行分词。
	 * @param key 文本标识
	 * @param context 输入文本
	 * @return 分词列表
	 * @throws IOException 
	 */
	public StringTuple analyzerTo(String key, String context) throws IOException {
		// 构造分词迭代器
		TokenStream stream = this.analyzer.tokenStream(key, new StringReader(context));
		CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
		stream.reset();
		
		// 读取分词
		StringTuple document = new StringTuple();
		while(stream.incrementToken()) {
			if(termAtt.length() > 0) {
				document.add(new String(termAtt.buffer(), 0, termAtt.length()));
			}
		}
		return document;
	}
	
	/**
	 * 关闭
	 */
	public void close() {
		this.analyzer.close();
	}
}
