package com.yhaitao.mahout.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 提供容器工具。
 * @author yanghaitao
 *
 */
public class CollectionsUtils {
	/**
	 * 将输入Map数据，根据value排序，返回排序list。
	 * @param mapData
	 * @return
	 */
	public static List<Entry<Integer, Integer>> sortByValue(Map<Integer, Integer> mapData) {
		List<Entry<Integer, Integer>> list = new ArrayList<Entry<Integer, Integer>>(mapData.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Entry<Integer, Integer> o1,
					Entry<Integer, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		return list;
	}
}
