FROM mysql:8.0

ENV MYSQL_ROOT_PASSWORD="password"
ENV MYSQL_DATABASE=metastore_db
ENV MYSQL_PASSWORD="password"

COPY init.sql /docker-entrypoint-initdb.d/

EXPOSE 3306

CMD [ "mysqld" ]