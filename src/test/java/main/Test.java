package main;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class Test {
	public static void main(String[] args) {
		// String url = "http://top.chinaz.com/hangye/index_zonghe_2.html";
//		String url = "[1507:2.7407537,1940:2.5121377]";
//		String substring = url.substring(1, url.length() - 1);
//		String[] split = substring.split(",");
//		for(String obj : split)
		System.err.println(Math.log10(1.0 + new Random().nextDouble()));
	}
	
	public static Map<String, Double> forUid(String uid, String scores) {
		String substring = scores.substring(1, scores.length() - 1);
		String[] split = substring.split(",");
		Map<String, Double> scoresMap = new HashMap<String, Double>();
		for(String obj : split) {
			String[] split2 = obj.split(":");
			scoresMap.put(split2[0] + "_" + uid, Double.valueOf(split2[1]));
		}
		return scoresMap;
	}
}
