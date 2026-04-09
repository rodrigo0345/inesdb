MAELSTROM_VER := 0.2.3
BIN_DIR := .bin
MAELSTROM := $(BIN_DIR)/maelstrom

$(MAELSTROM):
	mkdir -p $(BIN_DIR)
	curl -sSL -o $(BIN_DIR)/maelstrom.tar.bz2 https://github.com/jepsen-io/maelstrom/releases/download/v$(MAELSTROM_VER)/maelstrom.tar.bz2
	tar -xjf $(BIN_DIR)/maelstrom.tar.bz2 -C $(BIN_DIR) --strip-components=1
	rm $(BIN_DIR)/maelstrom.tar.bz2

build:
	go build -o node .

test: $(MAELSTROM) build
	$(MAELSTROM) test -w echo --bin ./node --node-count 3 --time-limit 10

clean:
	rm -rf $(BIN_DIR) node