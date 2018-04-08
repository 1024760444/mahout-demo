package com.yhaitao.mahout.driver.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ChinazDictionary implements Writable, DBWritable {
	private int dic_id;
	private String dic_name;
	
	public ChinazDictionary() {}
	public ChinazDictionary(int dic_id, String dic_name) {
		this.dic_id = dic_id;
		this.dic_name = dic_name;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
	    int index = 1;
	    statement.setInt(index++, this.getDic_id());
	    statement.setString(index++, this.getDic_name());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.setDic_id(resultSet.getInt(1));
		this.setDic_name(resultSet.getString(2));
	}

	@Override
	public void write(DataOutput out) throws IOException {}

	@Override
	public void readFields(DataInput in) throws IOException {}

	public int getDic_id() {
		return dic_id;
	}

	public void setDic_id(int dic_id) {
		this.dic_id = dic_id;
	}

	public String getDic_name() {
		return dic_name;
	}

	public void setDic_name(String dic_name) {
		this.dic_name = dic_name;
	}

}
