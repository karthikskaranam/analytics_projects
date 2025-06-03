-- Drop and recreate the 'SpotifyDB' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SpotifyDB')
BEGIN
    ALTER DATABASE SpotifyDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SpotifyDB;
END;
GO

-- Create the 'SpotifyDB' database
CREATE DATABASE SpotifyDB;
GO

USE SpotifyDB;
GO


IF OBJECT_ID('dbo.spotify_history', 'U') IS NOT NULL
    DROP TABLE dbo.spotify_history;
GO

-- Create the 'spotify_history' table

CREATE TABLE spotify_history(
    spotify_track_uri VARCHAR(255),
    ts DATETIME2,
    platform VARCHAR(50),
    milliseconds_played INT,
    track_name VARCHAR(255),
    artist_name VARCHAR(255),
    album_name VARCHAR(255),
    reason_start VARCHAR(50),
    reason_end VARCHAR(50),
    shuffle VARCHAR(50),
    skipto VARCHAR(50)
);
GO

TRUNCATE TABLE spotify_history;
GO

SELECT * FROM spotify_history;
-- Load the data from the CSV file into the 'spotify_history' table


BULK INSERT spotify_history
FROM 'D:\analytics_projects\spotify_analytics\data\spotify_history_data.csv'
WITH
(
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);
GO

