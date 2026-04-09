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

# Transaction workload: List append with transaction semantics
test-append: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-list-append \
	  --bin ./node \
	  --node-count 1 \
	  --concurrency 5 \
	  --time-limit 20 \
	  --rate 100

# Transaction workload: Read-write register with transaction semantics
test-rw-register: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-rw-register \
	  --bin ./node \
	  --node-count 1 \
	  --concurrency 5 \
	  --time-limit 20 \
	  --rate 100

# Stress test: High concurrency with txn-list-append
test-stress: $(MAELSTROM) build
	$(MAELSTROM) test -w txn-list-append \
	  --bin ./node \
	  --node-count 1 \
	  --concurrency 16 \
	  --time-limit 30 \
	  --rate 500

# Default test: Run transaction tests
test: test-append test-rw-register

clean:
	rm -rf $(BIN_DIR) node