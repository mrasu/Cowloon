CREATE DATABASE cowloon;
USE cowloon;

CREATE TABLE users (
  id        INT PRIMARY KEY AUTO_INCREMENT,
  tenant_id INT NOT NULL,
  name      VARCHAR(255)
);


CREATE TABLE messages (
  id      INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL,
  text    VARCHAR(255),
  FOREIGN KEY (user_id) REFERENCES users (id)
);

CREATE TABLE attachments (
  id         INT PRIMARY KEY AUTO_INCREMENT,
  message_id INT NOT NULL,
  path       VARCHAR(255),
  FOREIGN KEY (message_id) REFERENCES messages (id)
);


INSERT INTO users (tenant_id, name)
VALUES (1, 'user1-1'),
       (1, 'user1-2'),
       (2, 'user2-1');


INSERT INTO messages (user_id, text)
SELECT id, CONCAT('hello ', name)
FROM users;

INSERT INTO messages (user_id, text)
SELECT id, CONCAT('thanks, ', name)
FROM users
WHERE tenant_id = 1;


INSERT INTO attachments (message_id, path)
SELECT messages.id, CONCAT('/path/to/', CONVERT(messages.id, CHAR))
FROM messages;

INSERT INTO attachments (message_id, path)
SELECT messages.id, CONCAT('/path/to2/', CONVERT(messages.id, CHAR))
FROM messages
       INNER JOIN users on messages.user_id = users.id
where users.name = 'user1-1';

