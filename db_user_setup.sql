CREATE USER IF NOT EXISTS 'metrika_user'@'localhost' IDENTIFIED BY 'your_secure_password';

GRANT ALL PRIVILEGES ON yandex_metrika.* TO 'metrika_user'@'localhost';
FLUSH PRIVILEGES;