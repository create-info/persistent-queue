-- create shedlock_tab

CREATE TABLE `shedlock_tab` (
`lock_name` varchar(64) NOT NULL COMMENT 'lock key',
`lock_until` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'lock expiry time',
`locked_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'lock time',
`locked_by` varchar(255) NOT NULL COMMENT 'who lock',
PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;