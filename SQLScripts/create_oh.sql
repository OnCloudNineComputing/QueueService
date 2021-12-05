create schema `ohdb`;
create table `ohdb`.`OfficeHours`(
    `id` int(8) NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`ta_email` varchar(30),
	`ta_firstname` varchar(24),
	`ta_lastname` varchar(24),
    `location` varchar(24),
    `course_name` varchar(24),
    `course_number` varchar(24),
    `zoom_link` varchar(24),
    `start_time` time(0),
    `end_time` time(0),
    `start_date` DATE,
    `end_date` DATE,
    `oh_days` varchar(10)
);

INSERT INTO `ohdb`.`OfficeHours` VALUES(
	Null, "smr2218@columbia.edu", "Sam", "Raab", "CSC 451", "Intro to Databases", "id_of_course", "examplezoomlink.zom", '11:15:45', '11:45:45', '2021-01-22', '2021-03-12', "MT"
);

INSERT INTO `ohdb`.`OfficeHours` VALUES(
	Null, "rss2218@columbia.edu", "Rachel", "Sacks", "COMS 4995", "Nueral Netwroks", "id_of_course", "examplezoomlink.zom", '11:00:45', '12:15:00', '2021-01-22', '2021-03-12', "WF"
);