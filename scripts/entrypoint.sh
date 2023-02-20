#!/bin/sh

java -jar -Dspring.datasource.url=$DB_URL -Dspring.datasource.username=$DB_USERNAME -Dspring.datasource.password=$DB_PASSWORD /app/informix-access-layer.jar


