-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use Keyspace
USE mykeyspace;

-- Create Table
CREATE TABLE IF NOT EXISTS mytable (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT
);
