-- Drop the database (must be done outside a transaction block)
DROP DATABASE IF EXISTS my_data_warehouse;

-- Create the new database
CREATE DATABASE my_data_warehouse;

-- Connect to the new database (manual step in psql or connection settings in GUI)
-- \c my_data_warehouse   -- Uncomment this if using psql

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
