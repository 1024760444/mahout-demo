package com.yhaitao.mahout.lookalike.item;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Random;

import org.apache.mahout.cf.taste.common.TasteException;
import com.yhaitao.mahout.utils.CSVUtils;

/**
 * 基于广告的协同过滤推荐。
 * @author yhaitao
 *
 */
public class BaseOnAidTest {
	/**
	 * 合并推荐与测试。
	 * @param args
	 * @throws IOException
	 * @throws TasteException
	 */
	public static void main(String[] args) throws IOException, TasteException {
		String testCSVFile = "D:/yanghaitao/lookalike/preliminary_contest_data/test1.csv";
		String scoresPath = "D:/yanghaitao/lookalike/tmp/item/item2/";
		String submitFile = "D:/yanghaitao/lookalike/tmp/submission/submission.csv";
		
		/**
		 * 加载评分数据
		 * key : aid_uid
		 * value : score (通过对数处理，最多8位)
		 */
		Map<String, Double> scoresMap = CSVUtils.loadScores(scoresPath);
		
		// 输出文件
		File file = new File(submitFile);
		Writer fos = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
		BufferedWriter bw = new BufferedWriter(fos);
		
		// 对测试样例，每一个样例评分
		int mycount = 0;
		int good = 0;
		BufferedReader reader = new BufferedReader(new FileReader(testCSVFile));
		String line = null;
		while((line = reader.readLine()) != null) {
			String[] items = line.split(",");
			if(items != null && items.length >= 2) {
				int aid = CSVUtils.toInt(items[0], -2);
				int uid = CSVUtils.toInt(items[1], -2);
				String key = aid + "_" + uid;
				double scores = 0;
				if(scoresMap.containsKey(key)) {
					scores = scoresMap.get(key);
				}
				
				if(scores <= 0) {
					scores = Math.log10(1.0 + new Random().nextDouble());
				}
				
				if(scores >= 1) {
					scores = 0.9999;
				}
				
				if(aid == -2) {
					// aid,uid,score
					bw.append("aid,uid,score").append("\n");
				} else {
					// 2265990
					bw.append(aid + "," + uid + "," + CSVUtils.format(scores)).append("\n");
				}
				
				if(scores > 0.9) {
					good++;
				}
				
				mycount++;
				if(mycount%3000 == 0) {
					bw.flush();
				}
			}
		}
		bw.flush();
		bw.close();
		reader.close();
		System.err.println("mycount : " + mycount + ", good : " + good);
	}
}
