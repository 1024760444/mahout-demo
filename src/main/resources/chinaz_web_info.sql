/*
Navicat MySQL Data Transfer

Source Server         : hdp1-172.19.10.33
Source Server Version : 50173
Source Host           : 172.19.10.33:3306
Source Database       : kmeans

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2018-04-04 17:03:45
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for chinaz_web_info
-- ----------------------------
DROP TABLE IF EXISTS `chinaz_web_info`;
CREATE TABLE `chinaz_web_info` (
  `domain` varchar(255) NOT NULL COMMENT '网站域名',
  `name` varchar(255) DEFAULT NULL COMMENT '网站名称',
  `desc` varchar(1024) DEFAULT NULL COMMENT '网站描述',
  PRIMARY KEY (`domain`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='站长之家网页基本信息';

-- ----------------------------
-- Table structure for cluster_info
-- ----------------------------
DROP TABLE IF EXISTS `cluster_info`;
CREATE TABLE `cluster_info` (
  `cluster_type` int(1) NOT NULL DEFAULT '1' COMMENT '聚类类型。1 kmeans-cosine聚类， 2 kmeans-euclidean聚类',
  `cluster_id` int(10) NOT NULL COMMENT '聚类类别标识',
  `cluster_keys` int(10) NOT NULL COMMENT '聚类类别top20关键字编号',
  `keys_number` int(10) DEFAULT '0' COMMENT 'top20关键字出现次数',
  PRIMARY KEY (`cluster_type`,`cluster_id`,`cluster_keys`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='聚类类别信息';

-- ----------------------------
-- Table structure for cluster_kmeans_cosine
-- ----------------------------
DROP TABLE IF EXISTS `cluster_kmeans_cosine`;
CREATE TABLE `cluster_kmeans_cosine` (
  `cluster_type` int(1) NOT NULL DEFAULT '1' COMMENT '聚类类型。1 kmeans-cosine聚类， 2 kmeans-euclidean聚类',
  `cluster_id` int(10) NOT NULL COMMENT '聚类类别标识',
  `web_domain` varchar(255) NOT NULL COMMENT '聚类的向量标识，也就是文章标识。比如域名',
  PRIMARY KEY (`cluster_type`,`cluster_id`,`web_domain`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='kmeans聚类结果表';
