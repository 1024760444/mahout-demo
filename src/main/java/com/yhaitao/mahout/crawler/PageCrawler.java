package com.yhaitao.mahout.crawler;

import java.net.URL;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;

public class PageCrawler {
	/**
	 * 抓取网页文本。
	 * @param url
	 * @return
	 * @throws Exception 
	 */
	public static String crawler(String url) throws Exception {
		// 连接网页，建立分析工具
		Parser parser = new Parser(new URL(url).openConnection());
		
		/**
		 * 过滤规则
		 * 抓取标签文本： div title h1 h2 p a
		 */
		NodeFilter div = new TagNameFilter("div");
		NodeFilter title = new TagNameFilter("title");
		NodeFilter h1 = new TagNameFilter("h1");
		NodeFilter h2 = new TagNameFilter("h2");
		NodeFilter p = new TagNameFilter("p");
		NodeFilter a = new TagNameFilter("a");
		
		// 过滤
		NodeList nodeList = parser.extractAllNodesThatMatch(div);
		nodeList.add(parser.extractAllNodesThatMatch(h1));
		nodeList.add(parser.extractAllNodesThatMatch(h2));
		nodeList.add(parser.extractAllNodesThatMatch(p));
		nodeList.add(parser.extractAllNodesThatMatch(a));
		nodeList.add(parser.extractAllNodesThatMatch(title));
		
		// 获取最终文本
		StringBuffer context = new StringBuffer();
		int titlesize = nodeList.size();
		for (int i = 0; i < titlesize; i++) {
			Node elementAt = nodeList.elementAt(i);
			context.append(elementAt.toPlainTextString().trim() + " ");
		}
		return context.toString();
	}
}
