package com.yhaitao.mahout.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.yhaitao.mahout.lookalike.bean.TrainObj;

/**
 * CSV文件操作工具。
 * @author yhaitao
 *
 */
public class CSVUtils {
	public static void main(String[] args) throws IOException {
		String trainFile = "D:/yanghaitao/lookalike/preliminary_contest_data/train.csv";
		List<TrainObj> tranObjList = CSVUtils.readTrainData(trainFile);
		System.err.println(tranObjList.size());
	}
	
	/**
	 * 将csv训练数据，读取规范化后，写入到txt文件中。
	 * @param csvFile 训练数据文件路径
	 * @param toTxt 规范化数据
	 * @throws IOException 
	 */
	public static void readTrainDataTo(String csvFile, String toTxt) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(csvFile));
		String line = null;
		List<TrainObj> trainObjList = new ArrayList<TrainObj>();
		File txtFile = new File(toTxt);
		while((line = reader.readLine()) != null) {
			String[] items = line.split(",");
			if(items != null && items.length >= 3) {
				int aid = toInt(items[0], -2);
				int uid = toInt(items[1], -2);
				int label = toInt(items[2], -2);
				if(aid != -2 && uid != -2 && label != -2) {
					trainObjList.add(new TrainObj(aid, uid, label));
					if(trainObjList.size() >= 3000) {
						FileUtils.write(txtFile, forOutput(trainObjList), "UTF-8", true);
						trainObjList.clear();
					}
				}
			}
		}
		
		// 
		if(trainObjList.size() > 0) {
			FileUtils.write(txtFile, forOutput(trainObjList), "UTF-8", true);
			trainObjList.clear();
		}
		reader.close();
	}
	
	/**
	 * 读取训练数据文件。
	 * @param csvFile 训练数据文件路径
	 * @return 训练数据列表
	 * @throws IOException 
	 */
	public static List<TrainObj> readTrainData(String csvFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(csvFile));
		String line = null;
		List<TrainObj> trainObjList = new ArrayList<TrainObj>();
		while((line = reader.readLine()) != null) {
			String[] items = line.split(",");
			if(items != null && items.length >= 3) {
				int aid = toInt(items[0], -2);
				int uid = toInt(items[1], -2);
				int label = toInt(items[2], -2);
				if(aid != -2 && uid != -2 && label != -2) {
					trainObjList.add(new TrainObj(aid, uid, label));
				}
			}
		}
		reader.close();
		return trainObjList;
	}
	
	/**
	 * 字符串转换为数字。
	 * @param str
	 * @param def
	 * @return
	 */
	public static int toInt(String str, int def) {
		try {
			return Integer.valueOf(str);
		} catch (Exception e) {
			return def;
		}
	}
	
	/**
	 * 将数据拼接为协同过滤输入数据
	 * @param trainObj 训练数据
	 * @return 协同过滤输入数据： uid,aid,core
	 */
	private static String forOutput(List<TrainObj> trainObjList) {
		StringBuffer ouput = new StringBuffer();
		for(TrainObj obj : trainObjList) {
			ouput.append(obj.getUid()).append("\t")
			.append(obj.getAid()).append("\t")
			.append(getLabel(obj.getLabel())).append("\n");
		}
		return ouput.toString();
	}
	
	/**
	 * 随机评分。
	 * @param score
	 * @return
	 */
	private static int getLabel(int score) {
		if(score == 1) {
			return 10 + RANDOM.nextInt(5);
		} else {
			return RANDOM.nextInt(5);
		}
	}
	public static final Random RANDOM = new Random();
	

	/**
	 * 加载评分数据
	 * @param scoresPath
	 * @return
	 * @throws IOException
	 */
	public static Map<String, Double> loadScores(String scoresPath) throws IOException {
		File path = new File(scoresPath);
		File[] listFiles = path.listFiles();
		Map<String, Double> scoresMap = new HashMap<String, Double>();
		for(File sFile : listFiles) {
			BufferedReader reader = new BufferedReader(new FileReader(sFile));
			String line = null;
			while((line = reader.readLine()) != null) {
				// 92832	[1507:10.666783,1940:7.1456504]
				String[] split = line.split("\t");
				if(split != null && split.length >= 2) {
					scoresMap.putAll(forUidNotLog(split[0], split[1]));
				}
			}
			reader.close();
		}
		return scoresMap;
	}
	
	/**
	 * 数据拼接。
	 * @param uid
	 * @param scores
	 * @return
	 */
	public static Map<String, Double> forUidNotLog(String uid, String scores) {
		String substring = scores.substring(1, scores.length() - 1);
		String[] split = substring.split(",");
		Map<String, Double> scoresMap = new HashMap<String, Double>();
		for(String obj : split) {
			String[] split2 = obj.split(":");
			scoresMap.put(split2[0] + "_" + uid, format(Double.valueOf(split2[1])));
		}
		return scoresMap;
	}
	
	/**
	 * 数据拼接。
	 * @param uid
	 * @param scores
	 * @return
	 */
	public static Map<String, Double> forUid(String uid, String scores) {
		String substring = scores.substring(1, scores.length() - 1);
		String[] split = substring.split(",");
		Map<String, Double> scoresMap = new HashMap<String, Double>();
		for(String obj : split) {
			String[] split2 = obj.split(":");
			scoresMap.put(split2[0] + "_" + uid, format(Math.log10(Double.valueOf(split2[1]))));
		}
		return scoresMap;
	}
	
	/**
	 * 最多保留七位小数。
	 * @param indata
	 * @return
	 */
	public static double format(double indata) {
	    BigDecimal bigDecimal = new BigDecimal(indata);
	    return bigDecimal.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
	  }
}
