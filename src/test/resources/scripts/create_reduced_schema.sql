CREATE TABLE `customer` (
                                `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                `creationDate` datetime DEFAULT NULL,
                                `lastModifiedDate` datetime DEFAULT NULL,
                                `customerNumber` varchar(255) NOT NULL,
                                `emailAddress` varchar(255) NOT NULL,
                                `firstname` varchar(255) DEFAULT NULL,
                                `lastname` varchar(255) DEFAULT NULL,
                                `title` varchar(255) DEFAULT NULL,
                                `gender` varchar(255) DEFAULT NULL,
                                `version` bigint(20) NOT NULL DEFAULT '0',
                                PRIMARY KEY (`id`),
                                UNIQUE KEY `uk_cust_customerNumber` (`customerNumber`),
                                UNIQUE KEY `uk_cust_emailAddress` (`emailAddress`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `no_primary_key` (
                                  `creationDate` datetime DEFAULT NULL,
                                  `lastModifiedDate` datetime DEFAULT NULL,
                                  `emailAddress` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;