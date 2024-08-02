#!/bin/bash

echo "Starting multiple Go servers..."

# 启动第一个 Go 服务器
start  "Server 1" winpty bash  -c  "echo 'Registry server'; go run main.go -mode=server -operation=Delete -api=1"

# 启动第二个 Go 服务器
start "Server 2:localhost:9011" winpty bash -c "echo 'localhost:9011'; go run main.go -mode=server -addr=localhost:9011"

# 启动第三个 Go 服务器
start "Server 3:localhost:4561" winpty bash -c "echo 'localhost:4561'; go run main.go -mode=server -addr=localhost:4561"

# 启动第四个 Go 服务器
start "Server 4:localhost:8001" winpty bash -c "echo 'localhost:8001'; go run main.go -mode=server -addr=localhost:8001"

echo "All servers started."