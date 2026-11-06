-- Base "maintenance" : copie de la zone Gold, lue par Grafana
CREATE USER maintenance WITH PASSWORD 'maintenance';
CREATE DATABASE maintenance OWNER maintenance;
