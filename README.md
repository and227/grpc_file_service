Для запуска сервера:
```
# path - путь к хранилищу файлов
cd cmd/server
go build -o server main.go
FILES_STORAGE=<path> ./server                               
```

Для запуска клиента
```
# read_path - путь к файлам для загрузки на сервер
# write_path - путь для выгрузки файлов с сервера
cd cmd/client
go build -o client main.go
READ_FILES_FOLDER=<read_path> WRITE_FILES_FOLDER=<write_path> ./client
```
