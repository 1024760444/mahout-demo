package com.yhaitao.mahout.crawler.chinaz;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.mahout.crawler.bean.ChinazWeb;
import com.yhaitao.mahout.http.MarkHttpClient;

/**
 * 从站长之家（http://top.chinaz.com/hangye/）抓取网页域名及其描述。
 * @author yhaitao
 *
 */
public class ChinazCrawler {
	private final static Logger LOGGER = LoggerFactory.getLogger(ChinazCrawler.class);
	private static String url = "jdbc:mysql://172.19.10.33:3306/kmeans?characterEncoding=utf8";
	private static String uname = "root";
	private static String passwd = "123456";
	
	/**
	 * 数据插入语句。
	 */
	private static final String SAVE_SQL = "replace into chinaz_web_info (domain, class_name, web_name, web_desc) values (?, ?, ?, ?)";
	
	/**
	 * 抓取网页域名与网站描述。以域名为key，描述为value保存到内存Map中，最终写到本地文件。
	 * @param args 输入抓取数据保存文件
	 * jdbc:mysql://172.19.10.33:3306/kmeans?characterEncoding=utf8 root 123456
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		readOriginalFile();
		url = args[0];
		uname = args[1];
		passwd = args[2];
	}
	
	/**
	 * 原始数据文件。
	 * @return key文本标识，value文本内容
	 * @throws Exception 
	 */
	private static void readOriginalFile() throws Exception {
		MarkHttpClient httpClient = new MarkHttpClient();
		String baseUrl = "http://top.chinaz.com/hangye/";
		String response = httpClient.httpGet(baseUrl);
		getClassObject(response);
	}
	
	/**
	 * 抓取所有分类网页。
	 * @param response
	 * @throws Exception
	 */
	private static void getClassObject(String response) throws Exception {
		Pattern pa = Pattern.compile("<a class=\"TNMI-SubItem\" href=\"(.*?)\" >(.*?)</a>");
		Matcher ma = pa.matcher(response);
		while (ma.find()) {
			// 分类名称与基础网页
			String className = ma.group(2);
			String baseUrl = ma.group(1);
			baseUrl = baseUrl.startsWith("http:") ? baseUrl : ("http:" + baseUrl);
			if(baseUrl.endsWith(".html")) {
				getClassObject(baseUrl, className);
			}
			LOGGER.info("baseUrl : {}", baseUrl);
		}
	}
	
	/**
	 * 抓取单个分类的网页。
	 * @param url http://top.chinaz.com/hangye/index_jiaoyu.html
	 * @param className
	 * @throws Exception
	 */
	private static void getClassObject(String url, String className) throws Exception {
		MarkHttpClient httpClient = new MarkHttpClient();
		String response = httpClient.httpGet(url);
		int maxPage = getMaxPage(response);
		List<ChinazWeb> filterList = filterList(response, className);
		for(int i = 2; i <= maxPage; i++) {
			String get_url = url.substring(0, url.indexOf(".html")) + "_" + i + ".html";
			try {
				String httpGet = httpClient.httpGet(get_url);
				filterList.addAll(filterList(httpGet, className));
				LOGGER.info("get_url : {}, success ", get_url);
			} catch (Exception e) {
				LOGGER.info("get_url : {}, Excpetion : {}", get_url, e.getMessage());
			}
		}
		saveToMysql(filterList);
	}

	private static int getMaxPage(String response) {
		// <div class="ListPageWrap"></div>
		Pattern pa = Pattern.compile("<div class=\"ListPageWrap\">(.*?)</div>");
		Matcher ma = pa.matcher(response);
		ma.find();
		String pageNumberText = ma.group(1);
		int max = getNumber(pageNumberText);
		return max;
	}
	
	private static int getNumber(String text) {
		// <a class="Pagecurt" href="index_shenghuo.html">1</a>
		Pattern pa = Pattern.compile("<a href=\"(.*?)\">(.*?)</a>");
		Matcher ma = pa.matcher(text);
		int max = 0;
		while (ma.find()) {
			// 分类名称与基础网页
			int pN = toInt(ma.group(2), 0);
			if(pN > max) max = pN;
		}
		return max;
	}
	
	private static int toInt(String str, int def) {
		try {
			return Integer.valueOf(str);
		} catch (Exception e) {
			return def;
		}
	}

	/**
	 * 将数据保存到Mysql。
	 * @param filterList
	 * @throws Exception 
	 */
	public static void saveToMysql(List<ChinazWeb> filterList) throws Exception {
		Connection connection = null;
		PreparedStatement statement = null;
		connection = getConnection();
		connection.setAutoCommit(false);
		statement = connection.prepareStatement(SAVE_SQL);
		for (ChinazWeb webInfo : filterList) {
			webInfo.write(statement);
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
	
	/**
	 * 获取Mysql连接。
	 * @return
	 * @throws Exception
	 */
	private static Connection getConnection() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		return DriverManager.getConnection(url, uname, passwd);
	}
	
	/**
	 * 获取网页的标题。
	 * @param context 网页源码
	 * @return 网页的标题
	 */
	private static List<ChinazWeb> filterList(String context, String class_name) {
		List<ChinazWeb> webList = new ArrayList<ChinazWeb>();
		Pattern pa = Pattern.compile("<div class=\"CentTxt\">(.*?)<div class=\"RtCRateWrap\">");
		Matcher ma = pa.matcher(context);
		while (ma.find()) {
			String webContext = ma.group(1);
			String name = filter(webContext, "title='(.*?)'");
			String desc = filter(webContext, "<p class=\"RtCInfo\">(.*?)</p>");
			String domain = filter(webContext, "<span class=\"col-gray\">(.*?)</span>");
			ChinazWeb web = new ChinazWeb();
			web.setName(name);
			web.setDesc(desc.replaceAll("网站简介：", "").replaceAll(":", "").replaceAll("：", ""));
			web.setDomain(domain);
			web.setClass_name(class_name);
			webList.add(web);
		}
		return webList;
	}

	/**
	 * 正则匹配网页内容。
	 * @param context
	 * @param regex
	 * @return
	 */
	private static String filter(String context, String regex) {
		Pattern pa = Pattern.compile(regex);
		Matcher ma = pa.matcher(context);
        String title = null;
        while (ma.find()) {  
        	title = ma.group(1);
            if(title != null && !"".equals(title)) {
            	break;
            }
        }
		return title;
	}

}
