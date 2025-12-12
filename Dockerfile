FROM postgres:16-alpine

# Копируем все скрипты из папки init_scripts/ в специальную папку Docker
# Скрипты в /docker-entrypoint-initdb.d/ выполняются при первом запуске контейнера
COPY init_scripts/ /docker-entrypoint-initdb.d/