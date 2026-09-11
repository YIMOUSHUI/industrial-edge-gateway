cmake_minimum_required(VERSION 3.16)

project(industrial_edge_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(yaml-cpp REQUIRED)
find_package(spdlog REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(PahoMqttCpp REQUIRED)
find_package(SQLite3 REQUIRED)

add_executable(gatewayd
    src/main.cpp

    src/core/gateway_context.cpp
    src/core/telemetry_queue.cpp
    
    src/connectors/mock_connector.cpp
    src/connectors/tcp_connector.cpp

    src/converters/telemetry_converter.cpp

    src/mqtt/mqtt_publisher.cpp

    src/storage/offline_store.cpp

    src/common/config.cpp
    src/common/logger.cpp
)

add_executable(tcp_device_simulator
    tools/tcp_device_simulator.cpp
)

target_include_directories(gatewayd
    PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(gatewayd
    PRIVATE
    yaml-cpp
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    PahoMqttCpp::paho-mqttpp3
    SQLite::SQLite3
)

target_link_libraries(tcp_device_simulator
    PRIVATE
    nlohmann_json::nlohmann_json
)