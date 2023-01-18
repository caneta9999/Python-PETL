CREATE DATABASE IF NOT EXISTS PETL;
USE PETL;
SET SQL_MODE=ANSI_QUOTES;
CREATE TABLE IF NOT EXISTS SALES(
	id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
	product CHAR(100),
	quantity DECIMAL(2,0),
	price DECIMAL(5,2),
	date DATE,
	total DECIMAL(6,2) ,
	CAD_conversion DECIMAL(12,7)
);
CREATE TABLE IF NOT EXISTS EXCHANGES(
	id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
	date DATE,
	rate DECIMAL(6,5)
);
