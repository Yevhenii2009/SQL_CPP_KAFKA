FROM debian:bookworm

RUN apt-get update && \
    apt-get install -y \
        g++ \
        librdkafka-dev \
        nlohmann-json3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cpp/main.cpp .

RUN g++ -std=c++17 main.cpp -o processor \
    -lrdkafka++ \
    -lrdkafka

CMD ["./processor"]