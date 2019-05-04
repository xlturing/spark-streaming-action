/*
 Navicat MySQL Data Transfer

 Source Server         : localhost
 Source Server Version : 50627
 Source Host           : localhost
 Source Database       : log_analysis

 Target Server Version : 50627
 File Encoding         : utf-8

 Date: 12/19/2018 12:33:44 PM
*/

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `important_logs`
-- ----------------------------
DROP TABLE IF EXISTS `important_logs`;
CREATE TABLE `important_logs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_level` varchar(255) NOT NULL,
  `method` varchar(255) NOT NULL,
  `content` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `important_logs`
-- ----------------------------
BEGIN;
INSERT INTO `important_logs` VALUES ('1', '[warn]', 'calculate', '20180101-zero denominator warning!'), ('2', '[error]', 'readFile', '20180101-nullpointer file'), ('3', '[warn]', 'calculate', '20180102-zero denominator warning!'), ('4', '[error]', 'readFile', '20180102-nullpointer file');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
