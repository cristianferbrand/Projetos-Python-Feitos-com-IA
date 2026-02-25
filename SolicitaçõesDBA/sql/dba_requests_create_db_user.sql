-- Execute este script conectado ao banco 'postgres' como superusuário (ex.: postgres)
-- Ajuste a senha e a rede conforme necessário antes de rodar.
-- Cria o usuário e o banco principal do app.

-- Cria usuário (role) para o app
CREATE USER dba_app WITH PASSWORD 'suporte@123' LOGIN;

-- Cria o banco de dados e define o owner
CREATE DATABASE dba_requests OWNER dba_app;

-- (Opcional) Permissões adicionais
GRANT ALL PRIVILEGES ON DATABASE dba_requests TO dba_app;
