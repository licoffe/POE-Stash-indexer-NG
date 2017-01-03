all:
	@g++ -std=c++11 -Wall Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn

debug:
	@echo "Building debug version"
	@g++ -g -std=c++11 -Wall Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn

release:
	@echo "Building release"
	@g++ -std=c++11 -Wall Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn
	@echo "Packaging..."
	@mkdir release
	@mkdir release/data
	@cp schema.sql release
	@cp config-default.cfg release/config.cfg
	@cp indexer release
	@tar czvf release.tar.gz release