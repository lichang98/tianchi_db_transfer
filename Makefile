CC=g++
CFLAGS=-std=c++14 -O3
INC=-I. -I/usr/local/include
LIBS=-L /usr/local/lib
TEST_LINK=-lgtest -lgtest_main
LINK=-lpthread

transfer: ./out/main.cpp.o ./out/io_handler.cpp.o ./out/parser.cpp.o ./out/table_handler.cpp.o
	$(CC) ./out/main.cpp.o ./out/io_handler.cpp.o ./out/parser.cpp.o ./out/table_handler.cpp.o -o transfer $(LINK)

./out/main.cpp.o: ./src/main.cpp
	$(CC) $(INC) $(CFLAGS) -c ./src/main.cpp -o ./out/main.cpp.o

./out/table_handler.cpp.o: ./src/table_handler.cpp ./include/table_handler.h
	$(CC) $(INC) $(CFLAGS) -c ./src/table_handler.cpp -o ./out/table_handler.cpp.o

./out/parser.cpp.o: ./src/parser.cpp ./include/parser.h
	$(CC) $(INC) $(CFLAGS) -c ./src/parser.cpp -o ./out/parser.cpp.o
	
./out/io_handler.cpp.o: ./src/io_handler.cpp ./include/io_handler.h ./include/rapidjson/document.h ./include/rapidjson/writer.h ./include/rapidjson/stringbuffer.h
	$(CC) $(INC) $(CFLAGS) -c ./src/io_handler.cpp -o ./out/io_handler.cpp.o

clean:
	if [ -e transfer ];then rm transfer;fi
	if [ -z "$(ls -A ./out)" ];then rm -rf ./out/*;fi

.PHONY: clean

# Tests

./out/test_parser.cpp.o: ./test/test_parser.cpp /usr/local/include/gtest/gtest.h
	$(CC) $(INC) $(CFLAGS) -c ./test/test_parser.cpp -o ./out/test_parser.cpp.o

test_parser: ./out/test_parser.cpp.o ./out/parser.cpp.o
	$(CC) $(INC) $(LIBS) ./out/test_parser.cpp.o ./out/parser.cpp.o -o test_parser $(TEST_LINK)
	mv test_parser ./out/

.PHONY: test_parser

./out/test_io_handler.cpp.o: ./test/test_io_handler.cpp /usr/local/include/gtest/gtest.h
	$(CC) $(INC) $(CFLAGS) -c ./test/test_io_handler.cpp -o ./out/test_io_handler.cpp.o

test_io_handler: ./out/test_io_handler.cpp.o ./out/parser.cpp.o ./out/io_handler.cpp.o
	$(CC) $(INC) $(LIBS) ./out/test_io_handler.cpp.o ./out/parser.cpp.o ./out/io_handler.cpp.o -o test_io_handler $(TEST_LINK)
	mv test_io_handler ./out/

.PHONY: test_io_handler

./out/test_table_handler.cpp.o: ./test/test_table_handler.cpp /usr/local/include/gtest/gtest.h
	$(CC) $(INC) $(CFLAGS) -c ./test/test_table_handler.cpp -o ./out/test_table_handler.cpp.o

test_table_handler: ./out/test_table_handler.cpp.o ./out/parser.cpp.o ./out/io_handler.cpp.o ./out/table_handler.cpp.o
	$(CC) $(INC) $(LIBS) ./out/test_table_handler.cpp.o ./out/parser.cpp.o ./out/io_handler.cpp.o ./out/table_handler.cpp.o -o test_table_handler $(TEST_LINK)
	mv test_table_handler ./out/

.PHONY: test_table_handler