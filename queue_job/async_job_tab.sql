-- create async_job_tab
CREATE TABLE `async_job_tab` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `params` varchar(4096) COLLATE utf8mb4_unicode_ci DEFAULT '' NOT NULL,
 `job_type` int(11) NOT NULL,
 `job_status` tinyint(4) DEFAULT NULL COMMENT '0:init, 1:success, 2:failed, 3:continue, 4:retry',
 `start_time` int(11) unsigned DEFAULT '0',
 `create_time` int(11) unsigned NOT NULL,
 `update_time` int(11) unsigned NOT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
