CREATE DATABASE controltower;

CREATE SCHEMA ct;

-- controltower.ct.QBO_Universal_Token definition

CREATE TABLE ct.QBO_Universal_Token(TokenId BIGINT,
QBO_ClientId VARCHAR(MAX),
QBO_ClientSecret VARCHAR(MAX),
QBO_Environment VARCHAR(MAX),
RedirectUri VARCHAR(MAX),
CreatedAt VARCHAR(MAX),
UpdatedAt INTEGER);


-- controltower.ct.QBO_UserBased_AccessToken_Extraction definition

CREATE TABLE ct.QBO_UserBased_AccessToken_Extraction(TokenId BIGINT,
UserId BIGINT,
RealmId VARCHAR(MAX),
AccessToken VARCHAR(MAX),
RefreshToken VARCHAR(MAX),
CreatedAtUtc VARCHAR);


-- controltower.ct.QBO_UserBased_AccessToken_Migration definition

CREATE TABLE ct.QBO_UserBased_AccessToken_Migration(TokenId BIGINT,
UserId BIGINT,
RealmId VARCHAR(MAX),
AccessToken VARCHAR(MAX),
RefreshToken VARCHAR(MAX),
CreatedAtUtc VARCHAR);



-- controltower.ct.UserDatabase definition

CREATE TABLE ct.UserDatabase(UserId BIGINT,
CompanyName VARCHAR(MAX),
DatabaseName VARCHAR(MAX),
CreatedAt VARCHAR);



-- controltower.ct.UserLogs definition

CREATE TABLE ct.UserLogs(id BIGINT,
"level" VARCHAR(MAX),
message VARCHAR(MAX),
"timestamp" VARCHAR);


-- controltower.ct.Users definition

CREATE TABLE ct.Users(id VARCHAR(MAX),
userName VARCHAR(MAX),
userEmail VARCHAR(MAX),
userPassword VARCHAR(MAX),
pastPasswords VARCHAR(MAX),
userPhone VARCHAR(MAX),
userGender VARCHAR(MAX),
companyAddress VARCHAR(MAX),
companyName VARCHAR(MAX),
userProfilePic VARCHAR(MAX),
isActive bit,
createdAt VARCHAR(MAX),
updatedAt VARCHAR);

