package com.yhaitao.mahout.http;

import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * 
 * @author yhaitao
 *
 */
public class MarkHttpClient {

	/**
	 * HTTP请求客户端。
	 */
	private HttpClient httpClient;

	/**
	 * 网页请求时间，单位毫秒。
	 */
	private long timeOut;

	/**
	 * 初始化客户端。
	 */
	public MarkHttpClient() {
		httpClient = HttpClients.createDefault();
	}

	/**
	 * 爬取地址的网页内容。
	 * 
	 * @param url
	 *            网页地址
	 * @return 网页文本内容
	 * @throws Exception
	 *             爬取异常，或域名不存在、或请求超时、或协议错误等。
	 */
	public String httpGet(String url, int timeOut, String charset) throws Exception {
		this.timeOut = 0;
		HttpGet get = new HttpGet(url);
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeOut / 2)
				.setConnectTimeout(timeOut / 2).build();
		get.setConfig(requestConfig);
		long start = System.currentTimeMillis();
		HttpResponse httpResponse = httpClient.execute(get);
		this.timeOut = System.currentTimeMillis() - start;
		return parse(httpResponse, charset);
	}
	
	/**
	 * 爬取地址的网页内容， 默认超时2000毫秒。
	 * 
	 * @param url
	 *            网页地址
	 * @return 网页文本内容
	 * @throws Exception
	 *             爬取异常，或域名不存在、或请求超时、或协议错误等
	 */
	public String httpGet(String url) throws Exception {
		return httpGet(url, 48000, "UTF-8");
	}
	
	public String httpGet(String url, String charset) throws Exception {
		return httpGet(url, 48000, charset);
	}

	/**
	 * 获取请求响应时间。
	 * 
	 * @return 响应时间，单位毫秒
	 */
	public long getTimeOut() {
		return this.timeOut;
	}

	/**
	 * 解析网页响应。
	 * 
	 * @param httpResponse
	 *            网页响应
	 * @return 网页文本
	 * @throws IOException
	 * @throws ParseException
	 */
	private String parse(HttpResponse httpResponse, String charset) throws ParseException, IOException {
		int statusCode = httpResponse.getStatusLine().getStatusCode();
		String response = null;
		if (statusCode == HttpStatus.SC_OK) {
			response = EntityUtils.toString(httpResponse.getEntity());
			response = new String(response.getBytes(charset), "UTF-8");
		} else {
			throw new IOException(String.valueOf(statusCode));
		}
		return response;
	}
}
