CREATE DATABASE cowloon;
USE cowloon;
CREATE TABLE messages(id INT PRIMARY KEY AUTO_INCREMENT, text VARCHAR(255));

INSERT INTO messages(text) values('hello'), ('world');
