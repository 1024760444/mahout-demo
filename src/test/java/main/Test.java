package main;

public class Test {
	public static void main(String[] args) {
		// String url = "http://top.chinaz.com/hangye/index_zonghe_2.html";
		String url = "http://top.chinaz.com/hangye/index_zonghe.html";
		System.err.println(url.substring(0, url.indexOf(".html")) + "_" + 2 + ".html");
	}
}
