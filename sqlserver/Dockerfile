FROM mcr.microsoft.com/mssql/server:2022-latest

USER root
COPY init.sql /init.sql
RUN chmod 644 /init.sql
USER mssql