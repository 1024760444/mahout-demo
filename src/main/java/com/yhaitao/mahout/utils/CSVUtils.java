package com.yhaitao.mahout.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
			ouput.append(obj.getUid()).append(",")
			.append(obj.getAid()).append(",")
			.append((obj.getLabel() == 1) ? obj.getLabel() : 0).append("\n");
		}
		return ouput.toString();
	}
}
