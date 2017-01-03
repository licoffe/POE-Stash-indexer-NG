all:
	@echo "Building standard version"
	@g++ -std=c++11 -Wall Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn

debug:
	@echo "Building debug version"
	@g++ -g -std=c++11 -Wall Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn

clean:
	@echo "Cleaning up"
	@rm -Rf indexer release release.tar.gz

release:
	$(eval os := $(shell uname -ms | sed 's/ /-/g'))
	@echo Building release for $(os)
	@g++ -std=c++11 -Wall Misc.cpp CFG_reader.cpp main.cpp -Ofast -march=native -o indexer -lpthread -lcurl -lmysqlcppconn
	@echo "Packaging..."
	@mkdir POE-Stash-indexer-NG-$(os)
	@mkdir POE-Stash-indexer-NG-$(os)/data
	@cp config-default.cfg POE-Stash-indexer-NG-$(os)/config.cfg
	@cp schema.sql indexer README.md LICENSE POE-Stash-indexer-NG-$(os)
	@tar czvf POE-Stash-indexer-NG-$(os).tar.gz POE-Stash-indexer-NG-$(os)