drop table if exists `directory`;

CREATE TABLE `directory` (
  `id` bigint(20) unsigned NOT NULL auto_increment,
  `tablename` char(32) NOT NULL,
  `start` char(32) NOT NULL,
  `end` char(32) NOT NULL,
  `host` varchar(128) NOT NULL,
  `db` char(14) NOT NULL,
  `datatable` char(14) NOT NULL,
  `port` smallint NOT NULL,
  `created_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `retired_at` datetime default NULL,
  PRIMARY KEY  (`id`),
  UNIQUE KEY `UIDX_directory_tablename_datatable` (`tablename`,`datatable`),
  UNIQUE KEY `UIDX_directory_tablename_retired_at_end` (`tablename`,`retired_at`,`end`)
) ENGINE=innodb DEFAULT CHARSET=latin1;

drop table if exists `d_00000000000000`;

CREATE TABLE `d_00000000000000` (
  `k` char(37) NOT NULL,
  `v` blob,
  `modified_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL default '0000-00-00 00:00:00',
  PRIMARY KEY  (`k`)
) ENGINE=innodb DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/* if we have ideas about row size and/or num rows we should put them in here */
