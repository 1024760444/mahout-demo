package com.yhaitao.mahout.crawler.bean;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ChinazWeb implements DBWritable {
	/**
	 * 网站名称
	 */
	private String name;

	/**
	 * 网站描述
	 */
	private String desc;

	/**
	 * 网站域名
	 */
	private String domain;
	
	/**
	 * 网站分类
	 */
	private String class_name;
	
	public ChinazWeb() {}
	public ChinazWeb(String name, String desc, String domain, String class_name) {
		this.name = name;
		this.desc = desc;
		this.domain = domain;
		this.class_name = class_name;
	}
	
	@Override
	public void write(PreparedStatement statement) throws SQLException {
	    int index = 1;
	    statement.setString(index++, this.getDomain());
	    statement.setString(index++, this.getClass_name());
	    statement.setString(index++, this.getName());
	    statement.setString(index++, this.getDesc());
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getClass_name() {
		return class_name;
	}

	public void setClass_name(String class_name) {
		this.class_name = class_name;
	}
}