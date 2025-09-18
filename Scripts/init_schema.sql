/*
=============================================================
Create Database Schemas
=============================================================
Script Purpose:
    This script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    The database must already exist. 
    I dont know how to create a database from within a SQL script in PostgreSQL.
*/

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;
