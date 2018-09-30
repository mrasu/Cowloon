CREATE DATABASE cowloon;
USE cowloon;
CREATE TABLE messages(id INT PRIMARY KEY AUTO_INCREMENT, tenant_id INT, text VARCHAR(255));

INSERT INTO messages(tenant_id, text) values(3, 'PRESENT DAY'), (4, 'PRESENT TIME'), (3, 'HAHAHA');
