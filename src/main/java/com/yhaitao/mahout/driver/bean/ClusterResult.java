package com.yhaitao.mahout.driver.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 聚类数据对象。
 * @author yhaitao
 *
 */
public class ClusterResult implements Writable, DBWritable {
	/**
	 * 聚类类型。
	 * kmeans_cosine
	 * kmeans_euclidean
	 */
	private int cluster_type;
	
	/**
	 * 聚类类别编号
	 */
	private int cluster_id;
	
	/**
	 * 网站域名
	 */
	private String web_domain;
	
	public ClusterResult() {}
	public ClusterResult(int cluster_type, int cluster_id, String web_domain) {
		this.cluster_type = cluster_type;
		this.cluster_id = cluster_id;
		this.web_domain = web_domain;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
	    int index = 1;
	    statement.setInt(index++, this.getCluster_type());
	    statement.setInt(index++, this.getCluster_id());
	    statement.setString(index++, this.getWeb_domain());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.setCluster_type(resultSet.getInt(1));
		this.setCluster_id(resultSet.getInt(2));
		this.setWeb_domain(resultSet.getString(3));
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

	public String getWeb_domain() {
		return web_domain;
	}

	public void setWeb_domain(String web_domain) {
		this.web_domain = web_domain;
	}
}
