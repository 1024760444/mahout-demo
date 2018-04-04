package com.yhaitao.mahout.driver.reduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector.Element;

import com.yhaitao.mahout.driver.bean.ClusterInfo;
import com.yhaitao.mahout.driver.bean.ClusterResult;
import com.yhaitao.mahout.utils.CollectionsUtils;

/**
 * 输出分类信息到Mysql。
 * @author yanghaitao
 *
 */
public class ClusterResultReducer extends Reducer<IntWritable, WeightedVectorWritable, ClusterResult, NullWritable> {
	/**
	 * 数据插入语句。
	 */
	private static final String SAVE_SQL = "replace into cluster_info (cluster_type,cluster_id,cluster_keys,keys_number) values (?,?,?,?)";
	
	/**
	 * Mysql链接配置
	 */
	private DBConfiguration dbConf;

	/**
	 * 初始化数据连接。
	 */
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		dbConf = new DBConfiguration(conf);
	}
	
	/**
	 * 输入： 
	 * key ： 聚类编号
	 * value ： 聚类向量信息
	 * 输出：
	 * Mysql : cluster_type, cluster_id, web_domain
	 * Mysql : cluster_type, cluster_id, cluster_keys, keys_number
	 */
	protected void reduce(IntWritable key, Iterable<WeightedVectorWritable> values, Context context)
			throws IOException, InterruptedException {
		Map<Integer, Integer> keyNumber = new HashMap<Integer, Integer>();
		Iterator<WeightedVectorWritable> iterator = values.iterator();
		while(iterator.hasNext()) {
			WeightedVectorWritable next = iterator.next();
			NamedVector namedVector = (NamedVector) next.getVector();
			
			// 获取各个分词在聚类中出现次数
			Iterable<Element> nonZeroes = namedVector.nonZeroes();
			Iterator<Element> eleIter = nonZeroes.iterator();
			while(eleIter.hasNext()) {
				Element ele = eleIter.next();
				int index = ele.index();
				if(keyNumber.containsKey(index)) {
					keyNumber.put(index, keyNumber.get(index) + 1);
				} else {
					keyNumber.put(index, 1);
				}
			}
			
			// 保存域名与聚类编号的关系
			String name = namedVector.getName();
			context.write(new ClusterResult(1, key.get(), name), NullWritable.get());
		}
		
		// 对聚类中分词排序，取出现次数top20保存。
		saveKeyNumSorted(keyNumber, key.get());
		keyNumber.clear();
	}
	
	/**
	 * top20保存到数据库。
	 * @param keyNumber
	 * @throws InterruptedException 
	 */
	private void saveKeyNumSorted(Map<Integer, Integer> keyNumber, int cluster_id) throws InterruptedException {
		// 排序
		List<Entry<Integer, Integer>> keyNumSorted = CollectionsUtils.sortByValue(keyNumber);

		// 插入mysql
		try {
			Connection connection = null;
			PreparedStatement statement = null;
			// cluster_type, cluster_id, cluster_keys, keys_number
			connection = this.dbConf.getConnection();
			connection.setAutoCommit(false);
			statement = connection.prepareStatement(SAVE_SQL);
			for (int index = 0; index < 20; index++) {
				Entry<Integer, Integer> entry = keyNumSorted.get(index);
				ClusterInfo info = new ClusterInfo(1, cluster_id, entry.getKey(), entry.getValue());
				info.write(statement);
				statement.addBatch();
			}
			statement.executeBatch();
			connection.commit();
			close(connection, statement);
		} catch (Exception e) {
			throw new InterruptedException(e.getMessage());
		}
		
		// 清空
		keyNumSorted.clear();
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
}
