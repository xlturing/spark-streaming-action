/*
 Navicat MySQL Data Transfer

 Source Server         : localhost
 Source Server Version : 50627
 Source Host           : localhost
 Source Database       : monitor_alarm

 Target Server Version : 50627
 File Encoding         : utf-8

 Date: 12/11/2018 22:07:30 PM
*/

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `alarms`
-- ----------------------------
DROP TABLE IF EXISTS `alarms`;
CREATE TABLE `alarms` (
  `alarm_id` int(11) NOT NULL AUTO_INCREMENT,
  `game_id` int(11) NOT NULL,
  `game_name` varchar(255) NOT NULL,
  `words` varchar(255) NOT NULL,
  `words_freq` varchar(255) NOT NULL,
  `rule_id` int(11) NOT NULL,
  `rule_name` varchar(255) NOT NULL,
  `has_sent` int(11) NOT NULL,
  `is_problem` int(11) NOT NULL,
  `add_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`alarm_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `alarms`
-- ----------------------------
BEGIN;
INSERT INTO `alarms` VALUES ('1', '2301', '王者荣耀', '垃圾 连跪 坑 平衡性差 抄袭', '垃圾:3 坑:8 抄袭:0 连跪:0 平衡性差:0 ', '2', '平衡性问题', '0', '-1', '2018-12-11 21:40:06');
COMMIT;

-- ----------------------------
--  Table structure for `games`
-- ----------------------------
DROP TABLE IF EXISTS `games`;
CREATE TABLE `games` (
  `game_id` int(11) NOT NULL,
  `game_name` varchar(255) NOT NULL,
  PRIMARY KEY (`game_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `games`
-- ----------------------------
BEGIN;
INSERT INTO `games` VALUES ('2301', '王者荣耀'), ('2318', '穿越火线-枪战王者'), ('43639', '我的世界'), ('49995', '第五人格'), ('57312', '魂武者'), ('58881', '香肠派对'), ('59520', '明日之后'), ('70056', '绝地求生：刺激战场'), ('74838', '贪婪洞窟2'), ('83188', '方舟：生存进化');
COMMIT;

-- ----------------------------
--  Table structure for `monitor_games`
-- ----------------------------
DROP TABLE IF EXISTS `monitor_games`;
CREATE TABLE `monitor_games` (
  `game_id` int(11) NOT NULL,
  `game_name` varchar(200) NOT NULL,
  PRIMARY KEY (`game_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `monitor_games`
-- ----------------------------
BEGIN;
INSERT INTO `monitor_games` VALUES ('2301', '王者荣耀'), ('49995', '第五人格');
COMMIT;

-- ----------------------------
--  Table structure for `rules`
-- ----------------------------
DROP TABLE IF EXISTS `rules`;
CREATE TABLE `rules` (
  `rule_id` int(11) NOT NULL AUTO_INCREMENT,
  `rule_name` varchar(255) NOT NULL,
  `game_id` int(11) NOT NULL,
  `game_name` varchar(255) NOT NULL,
  `threshold` int(11) NOT NULL,
  `words` varchar(255) NOT NULL,
  `type` int(11) NOT NULL,
  `state` int(11) NOT NULL,
  PRIMARY KEY (`rule_id`)
) ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `rules`
-- ----------------------------
BEGIN;
INSERT INTO `rules` VALUES ('1', '更新问题', '2301', '王者荣耀', '2', '更新出错 无法登陆', '1', '0'), ('2', '平衡性问题', '2301', '王者荣耀', '10', '垃圾 连跪 坑 平衡性差 抄袭', '1', '0'), ('3', '网络问题', '2301', '王者荣耀', '7', '网络问题 卡顿 卡 掉线', '1', '0'), ('4', '平衡性问题', '49995', '第五人格', '10', '玩法单一 平衡性', '1', '0'), ('5', '程序问题', '49995', '第五人格', '5', 'bug 卸载', '0', '0');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
