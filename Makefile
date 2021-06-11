CC=g++
CFLAGS=-std=c++14
INC=-I .

transfer: ./out/main.cpp.o ./out/io_handler.cpp.o ./out/parser.cpp.o ./out/table_handler.cpp.o
	$(CC) ./out/main.cpp.o ./out/io_handler.cpp.o ./out/parser.cpp.o ./out/table_handler.cpp.o -o transfer 

./out/main.cpp.o: ./src/main.cpp
	$(CC) $(INC) -c ./src/main.cpp -o ./out/main.cpp.o

./out/table_handler.cpp.o: ./src/table_handler.cpp ./include/table_handler.h
	$(CC) $(INC) -c ./src/table_handler.cpp -o ./out/table_handler.cpp.o

./out/parser.cpp.o: ./src/parser.cpp ./include/parser.h
	$(CC) $(INC) -c ./src/parser.cpp -o ./out/parser.cpp.o
	
./out/io_handler.cpp.o: ./src/io_handler.cpp ./include/io_handler.h ./include/rapidjson/document.h ./include/rapidjson/writer.h ./include/rapidjson/stringbuffer.h
	$(CC) $(INC) -c ./src/io_handler.cpp -o ./out/io_handler.cpp.o

.PHONY: clean
clean:
	if [ -e transfer ];then rm transfer;rm -rf ./out/*;fi