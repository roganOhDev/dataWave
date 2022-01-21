-- db connections dump

CREATE TABLE `connections` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `uuid` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `db_type` varchar(255) NOT NULL,
  `host` varchar(255) NOT NULL,
  `port` varchar(255) NOT NULL,
  `account` varchar(255) DEFAULT NULL,
  `user` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL,
  `database` varchar(255) DEFAULT NULL,
  `warehouse` varchar(255) DEFAULT NULL,
  `option` varchar(255) DEFAULT NULL,
  `role` varchar(255) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- db elt_maps dump

CREATE TABLE `elt_maps` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `uuid` varchar(255) NOT NULL,
  `job_uuid` varchar(255) NOT NULL,
  `integrate_connection_uuid` varchar(255) NOT NULL,
  `destination_connection_uuid` varchar(255) NOT NULL,
  `table_list_uuids` varchar(255) NOT NULL,
  `active` tinyint NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- db job_infoes dump

CREATE TABLE `job_infoes` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `uuid` varchar(255) NOT NULL,
  `job_uuid` varchar(255) NOT NULL,
  `owner` varchar(255) NOT NULL,
  `schedule_interval` varchar(255) NOT NULL,
  `csv_files_directory` varchar(255) NOT NULL,
  `start_date` varchar(255) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- db schedule_logs dump

CREATE TABLE `schedule_logs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `uuid` varchar(255) NOT NULL,
  `job_uuid` varchar(255) NOT NULL,
  `result` varchar(255) NOT NULL,
  `error_message` varchar(255) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- db table_list dump

CREATE TABLE `table_list` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `uuid` varchar(255) NOT NULL,
  `connection_uuid` varchar(255) NOT NULL,
  `column_info` varchar(255) NOT NULL,
  `max_pk` bigint DEFAULT '0',
  `created_at` datetime(6) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;