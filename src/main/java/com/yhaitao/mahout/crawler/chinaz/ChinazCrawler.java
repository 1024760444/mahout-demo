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
	private static final String SAVE_SQL = "replace into chinaz_web_info (domain, web_name, web_desc) values (?, ?, ?)";
	
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
	 */
	private static void readOriginalFile() {
		MarkHttpClient httpClient = new MarkHttpClient();
		String baseUrl = "http://top.chinaz.com/hangye/";
		for(int i = 1; i <= 1881; i++) {
			String crawlerUrl = (i == 1) ? baseUrl : (baseUrl + "index_" + i + ".html");
			try {
				String response = httpClient.httpGet(crawlerUrl);
				List<ChinazWeb> filterList = filterList(response);
				// FileUtils.write(new File(fileName), GSON.toJson(filterList) + "\n", "UTF-8", true);
				saveToMysql(filterList);
				filterList.clear();
				LOGGER.info("httpGet : {}, success. ", crawlerUrl);
			} catch (Exception e) {
				LOGGER.warn("httpGet : {}, Exception : {}. ", crawlerUrl, e.getMessage());
				continue ;
			}
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
			statement.setString(1, webInfo.getDomain());
		    statement.setString(2, webInfo.getName());
		    statement.setString(3, webInfo.getDesc());
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
	private static List<ChinazWeb> filterList(String context) {
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
