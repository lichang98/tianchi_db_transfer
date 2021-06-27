main: main.cpp
	g++ main.cpp -std=c++14 -o main -O3 -lpthread -Wno-unused-result

.PHONY: clean
clean:
	if [ -e main ]; then rm main; fi