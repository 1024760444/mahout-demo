package main;

import java.net.URL;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;

public class HTMLParserTest {
	public static void main(String[] args) throws Exception {
		String url = "https://mbd.baidu.com/newspage/data/landingsuper?context=%7B%22nid%22%3A%22news_14871564879661384312%22%7D";
		Parser parser = new Parser(new URL(url).openConnection());
		
		// div title h1 h2 p a
		NodeFilter titlefilter = new TagNameFilter("a");
		NodeList titlenodes = parser.extractAllNodesThatMatch(titlefilter);
		int titlesize = titlenodes.size();
		for (int i = 0; i < titlesize; i++) {
			Node elementAt = titlenodes.elementAt(i);
			System.err.println(elementAt.toPlainTextString().trim());
		}
	}
}
