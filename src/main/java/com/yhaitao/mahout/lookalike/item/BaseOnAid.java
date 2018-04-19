package com.yhaitao.mahout.lookalike.item;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Random;

import org.apache.mahout.cf.taste.common.TasteException;

import com.opencsv.CSVWriter;
import com.yhaitao.mahout.utils.CSVUtils;

/**
 * 基于广告的协同过滤推荐。
 * @author yhaitao
 *
 */
public class BaseOnAid {
	/**
	 * 合并推荐与测试。
	 * @param args
	 * @throws IOException
	 * @throws TasteException
	 */
	public static void main(String[] args) throws IOException, TasteException {
		String testCSVFile = "D:/yanghaitao/lookalike/preliminary_contest_data/test1.csv";
		String scoresPath = "D:/yanghaitao/lookalike/tmp/item/scores/";
		String submitFile = "D:/yanghaitao/lookalike/tmp/item/submission.csv";
		
		/**
		 * 加载评分数据
		 * key : aid_uid
		 * value : score (通过对数处理，最多8位)
		 */
		Map<String, Double> scoresMap = CSVUtils.loadScores(scoresPath);
		
		// 输出文件
		File file = new File(submitFile);
		Writer writer = new FileWriter(file);
		CSVWriter csvWriter = new CSVWriter(writer, ',');
		
		// 对测试样例，每一个样例评分
		int mycount = 0;
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
				} else {
					scores = Math.log10(1.0 + new Random().nextDouble());
				}
				
				if(aid == -2) {
					// aid,uid,score
					String[] strs = {
							String.valueOf("aid"), 
							String.valueOf("uid"), 
							String.valueOf("score")};
					csvWriter.writeNext(strs);
				} else {
					// 2265990
					String[] strs = {
							String.valueOf(aid), 
							String.valueOf(uid), 
							String.valueOf(CSVUtils.format(scores))};
					csvWriter.writeNext(strs);
				}
				
				mycount++;
				if(mycount%3000 == 0) {
					csvWriter.flush();
				}
			}
		}
		csvWriter.flush();
		csvWriter.close();
		reader.close();
		System.err.println("mycount : " + mycount);
	}
	
	/**
	 * 规范化协同过滤输入。
	 * @throws IOException
	 */
	public static void test() throws IOException {
		 String modelFile = "D:/yanghaitao/lookalike/tmp/item/train.txt";
		 String trainFile = "D:/yanghaitao/lookalike/preliminary_contest_data/train.csv";
		
		// 模型数据
		 CSVUtils.readTrainDataTo(trainFile, modelFile);
	}
}
