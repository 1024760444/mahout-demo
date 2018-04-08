package main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TestSortMapByValue {
	public static void main(String[] args) {
//		String line = "wallpaper.desktx.com	桌面天下-桌面壁纸频道 桌面天下-桌面壁纸频道是壁纸爱好者和壁纸收藏家的乐园，在这里您每天都可以下载到最新的多种分辨率的无水印桌面壁纸；桌面天下桌面壁纸栏目打造国内最专业的壁纸下载旗舰站...";
//		String[] split = line.split("\t");
//		System.err.println(split[0]);
//		System.err.println(split[1]);
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		map.put(28067, 33);
		map.put(8280, 56);
		map.put(26528, 22);
		map.put(5772, 19);
		map.put(5773, 19);

		List<Entry<Integer, Integer>> list = sortByValue(map);
		for (Entry<Integer, Integer> e : list) {
			System.out.println(e.getKey() + ":" + e.getValue());
		}
	}

	public static List<Entry<Integer, Integer>> sortByValue(
			Map<Integer, Integer> mapData) {
		List<Entry<Integer, Integer>> list = new ArrayList<Entry<Integer, Integer>>(
				mapData.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Entry<Integer, Integer> o1,
					Entry<Integer, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		return list;
	}
}
