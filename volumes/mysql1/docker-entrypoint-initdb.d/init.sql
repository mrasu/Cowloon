CREATE DATABASE cowloon;
USE cowloon;
CREATE TABLE messages(id INT PRIMARY KEY AUTO_INCREMENT, tenant_id INT, text VARCHAR(255));

INSERT INTO messages(tenant_id, text) values(1, 'hello'), (2, 'world');
