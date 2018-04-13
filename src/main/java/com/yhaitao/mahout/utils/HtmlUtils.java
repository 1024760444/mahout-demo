package com.yhaitao.mahout.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.htmlparser.Parser;
import org.htmlparser.util.ParserException;
import org.htmlparser.visitors.TextExtractingVisitor;

/**
 * 正则表达式获取普通网页文本。
 * @author yhaitao
 *
 */
public class HtmlUtils {
	/**
	 * 网页meta标签内容提取
	 */
	private static final String REGEX_META = "<meta name=\"(.*?)\" content=\"(.*?)\" />";

	/**
	 * 网页script标签过滤
	 */
	private static final String REGEX_SCRIPT = "<script[^>]*?>[\\s\\S]*?<\\/script>";
	
	/**
	 * 网页style标签过滤
	 */
	private static final String REGEX_STYLE = "<style[^>]*?>[\\s\\S]*?<\\/style>";
	
	/**
	 * 获取网页链接的正则表达式
	 */
	private static final String HREF_REGULAR = "<a\\s.*?href=\"([^\"]+)\"[^>]*>(.*?)</a>";
	
	/**
	 * 过滤HTML网页的meta标签。
	 * @param context HTML文本
	 * @return meta对象数据对
	 */
	public static Map<String, String> filterMeta(String context) {
		Pattern pattern = Pattern.compile(REGEX_META);
		Matcher matcher = pattern.matcher(context);
		Map<String, String> metaMap = new HashMap<String, String>();
		while (matcher.find()) {
			String key = matcher.group(1);
			String value = matcher.group(2);
			if (key != null && !"".equals(key)) {
				metaMap.put(key, value);
			}
		}
		return metaMap;
	}
	
	/**
	 * 过滤HTML网页的css文件。
	 * @param context HTML文本
	 * @return 去掉了css的文本
	 */
	public static String filterStyle(String context) {
		if(context == null) {
			return null;
		}
		Pattern pattern = Pattern.compile(REGEX_STYLE);
		Matcher matcher = pattern.matcher(context);
		String result = matcher.replaceAll("");
		return result;
	}
	
	/**
	 * 过滤HTML网页的script标签。
	 * @param context HTML文本
	 * @return 去掉了Script的文本
	 */
	public static String filterScript(String context) {
		if(context == null) {
			return null;
		}
		Pattern pattern = Pattern.compile(REGEX_SCRIPT);
		Matcher matcher = pattern.matcher(context);
		String result = matcher.replaceAll("");
		return result;
	}

	/**
	 * 过滤HTML网页的html标签。
	 * @param context HTML文本
	 * @return 去掉了HTML其他标签的文本
	 * @throws ParserException
	 */
	public static String filterHTML(String context) throws ParserException {
		String result = null;
		Parser parser = null;
		try {
			parser = new Parser(context);
		} catch (Exception e) {
			return null;
		}
		TextExtractingVisitor nodeVisitor = new TextExtractingVisitor();
		parser.visitAllNodesWith(nodeVisitor);
		result = nodeVisitor.getExtractedText();
		return result;
	}
	
	/**
	 * 获取网页的标题。
	 * @param context 网页源码
	 * @return 网页的标题
	 */
	public static String filterTitle(String context) {
        Pattern pa = Pattern.compile("<title>(.*?)</title>");
        Matcher ma = pa.matcher(context);  
        String title = "";
        while (ma.find()) {  
        	title = ma.group(1);
            if(title != null && !"".equals(title)) {
            	break;
            }
        }
		return title;
	}
	
	/**
	 * 获取文章H1标签所有文本。
	 * @param context 网页文本
	 * @return 一级标题文本
	 */
	public static List<String> filterH1(String context) {
		Pattern pattern = Pattern.compile("<h1(.*?)>(.*?)</h1>");
		Matcher matcher = pattern.matcher(context);
		List<String> h1List = new ArrayList<String>();
		while (matcher.find()) {
			String value = matcher.group(2);
			if (value != null && !"".equals(value)) {
				h1List.add(value);
			}
		}
		return h1List;
	}
	
	/**
	 * 加重文本。
	 * @param context 网页文本
	 * @return 网页中加重文本
	 */
	public static List<String> filterStrong(String context) {
		Pattern pattern = Pattern.compile("<strong(.*?)>(.*?)</strong>");
		Matcher matcher = pattern.matcher(context);
		List<String> h1List = new ArrayList<String>();
		while (matcher.find()) {
			String value = matcher.group(2);
			if (value != null && !"".equals(value)) {
				h1List.add(value);
			}
		}
		return h1List;
	}
	
	/**
	 * 获取网页文本中Meta标签的文本。
	 * @param context 网页文本
	 * @return Meta标签文本列表
	 */
	public static List<String> filterMeta1(String context) {
		Pattern pattern = Pattern.compile("<meta(.*?)>");
		Matcher matcher = pattern.matcher(context);
		List<String> h1List = new ArrayList<String>();
		while (matcher.find()) {
			String value = matcher.group(1);
			if (value != null && !"".equals(value)) {
				h1List.add(value);
			}
		}
		return h1List;
	}
	
	/**
	 * 获取网页中的所有链接。
	 * @param context 网页
	 * @return 所有链接
	 */
	public static List<String> getPageUrls(String context) {
		List<String> urlList = new ArrayList<String>();
		 Pattern pattern = Pattern.compile(HREF_REGULAR);
		Matcher matcher = pattern.matcher(context);
		while (matcher.find()) {
			String url = matcher.group(1);
			if(url != null && !"".equals(url)) {
				urlList.add(url);
			}
		}
		return urlList;
	}
	
	/**
	 * 获取网页编码格式
	 * @param context 网页文本
	 * @return 网页编码格式
	 */
	public static String getCharSet(String context) {
		String charSet = "UTF-8";
		String regex = ".*charset=([^;]*).*";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(context);
		if (matcher.find()) {
			charSet = matcher.group(1);
		}
		return charSet;
	}
	
	/**
	 * 正则表达式，获取指定序号的字符串。
	 * @param regex 表达式
	 * @param context 匹配内容
	 * @param index 获取数据序号
	 * @return 指定序号的字符串
	 */
	public static String intercept(String regex, String context, int index) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(context);
		String words_str = null;
		while (matcher.find()) {
			words_str = matcher.group(index);
		}
		return words_str;
	}
	
	/**
	 * 判断输入字符串是否存在乱码。
	 * @param source 输入字符串
	 * @return 存在乱码返回false，不存在乱码返回true。
	 */
	public static boolean cclm(String source) {
		boolean flag = true;
		char ws[] = new char[] { '"', '?', ' ', '\'', '&' };
		for (int i = 0; i < source.length(); i++) {
			char c = source.charAt(i);
			for (int j = 0; j < ws.length; j++) {
				char v = ws[j];
				if (c == v) {
					flag = false;
				}
			}
			if ((int) c == 0xfffd) {
				flag = false;
			}
		}
		return flag;
	}
}
