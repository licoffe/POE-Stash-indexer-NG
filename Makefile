all:
	@echo "Choose OS: MacOS (make macos), Linux (make linux)"

macos:
	@echo "Building for MacOS"
	@g++ -std=c++11 -Wall -I/usr/local/include -I/usr/local/include/cppconn -L/usr/local/lib -lcurl -lmysqlcppconn Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer
	
linux:
	@echo "Building for Linux"
	@g++ -std=c++11 -Wall -I/usr/local/include -I/usr/include  -I/usr/local/include/cppconn -L/usr/local/lib Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn