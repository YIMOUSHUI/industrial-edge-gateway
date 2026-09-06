cmake_minimum_required(VERSION 3.16)

project(industrial_edge_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(yaml-cpp REQUIRED)
find_package(spdlog REQUIRED)

add_executable(gatewayd
    src/main.cpp
    src/connectors/mock_connector.cpp
    src/connectors/tcp_connector.cpp
    src/core/gateway_context.cpp
    src/common/config.cpp
    src/common/logger.cpp
)

target_include_directories(gatewayd
    PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(gatewayd
    PRIVATE
    yaml-cpp
    spdlog::spdlog
)
