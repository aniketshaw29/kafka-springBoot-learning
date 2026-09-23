-- PostgreSQL init script — creates separate schemas for each microservice
-- This simulates the "database per service" pattern in microservices

-- Order Service schema
CREATE SCHEMA IF NOT EXISTS order_service;

-- Kitchen Service schema
CREATE SCHEMA IF NOT EXISTS kitchen_service;

-- Notification Service schema
CREATE SCHEMA IF NOT EXISTS notification_service;

-- Grant permissions
GRANT ALL ON SCHEMA order_service TO fooddelivery;
GRANT ALL ON SCHEMA kitchen_service TO fooddelivery;
GRANT ALL ON SCHEMA notification_service TO fooddelivery;
