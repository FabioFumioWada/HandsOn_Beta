-- Remove o schema inteiro e todos os seus objetos (tabelas, views, etc.)
DROP SCHEMA IF EXISTS handson_beta.bronze CASCADE;
DROP SCHEMA IF EXISTS handson_beta.bronze_01 CASCADE;
DROP SCHEMA IF EXISTS handson_beta.bronze_02 CASCADE;
DROP SCHEMA IF EXISTS handson_beta.bronze_03 CASCADE;
DROP SCHEMA IF EXISTS handson_beta.bronze_meta CASCADE;



-- Recria o schema limpo
--CREATE SCHEMA IF NOT EXISTS handson_beta.bronze;