package com.yhaitao.mahout.driver.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ClusterInfo implements Writable, DBWritable {
	/**
	 * 聚类类型。
	 * 1 kmeans_cosine
	 * 2 kmeans_euclidean
	 */
	private int cluster_type;
	
	/**
	 * 聚类类别编号
	 */
	private int cluster_id;
	
	/**
	 * 聚类类别关键词
	 */
	private int cluster_keys;
	
	/**
	 * 关键字出现次数
	 */
	private int keys_number;
	
	public ClusterInfo() {}
	public ClusterInfo(int cluster_type, int cluster_id, int cluster_keys, int keys_number) {
		this.cluster_type = cluster_type;
		this.cluster_id = cluster_id;
		this.cluster_keys = cluster_keys;
		this.keys_number = keys_number;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
	    int index = 1;
	    statement.setInt(index++, this.getCluster_type());
	    statement.setInt(index++, this.getCluster_id());
	    statement.setInt(index++, this.getCluster_keys());
	    statement.setInt(index++, this.getKeys_number());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.setCluster_type(resultSet.getInt(1));
		this.setCluster_id(resultSet.getInt(2));
		this.setCluster_keys(resultSet.getInt(3));
		this.setKeys_number(resultSet.getInt(4));
	}

	@Override
	public void write(DataOutput out) throws IOException {}

	@Override
	public void readFields(DataInput in) throws IOException {}

	public int getCluster_type() {
		return cluster_type;
	}

	public void setCluster_type(int cluster_type) {
		this.cluster_type = cluster_type;
	}

	public int getCluster_id() {
		return cluster_id;
	}

	public void setCluster_id(int cluster_id) {
		this.cluster_id = cluster_id;
	}

	public int getCluster_keys() {
		return cluster_keys;
	}

	public void setCluster_keys(int cluster_keys) {
		this.cluster_keys = cluster_keys;
	}

	public int getKeys_number() {
		return keys_number;
	}

	public void setKeys_number(int keys_number) {
		this.keys_number = keys_number;
	}
	
}
