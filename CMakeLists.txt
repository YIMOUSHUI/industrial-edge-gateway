cmake_minimum_required(VERSION 3.16)

project(industrial_edge_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(yaml-cpp REQUIRED)
find_package(spdlog REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(PahoMqttCpp REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(PkgConfig REQUIRED)

# gatewayd在Ubuntu和ARM板端都需要libmodbus，
# 因此libmodbus不能只在非交叉编译环境中查找。\
pkg_check_modules(
    LIBMODBUS
    REQUIRED
    IMPORTED_TARGET
    libmodbus
)

add_executable(gatewayd
    src/main.cpp

    src/control/gateway_control_server.cpp

    src/core/gateway_context.cpp
    src/core/telemetry_queue.cpp
    src/core/processed_telemetry_queue.cpp
    src/core/command_queue.cpp
    src/core/command_router.cpp

    src/connectors/mock_connector.cpp
    src/connectors/tcp_connector.cpp
    src/connectors/can_connector.cpp
    src/connectors/modbus_connector.cpp

    src/converters/telemetry_converter.cpp
    src/converters/command_converter.cpp

    src/mqtt/mqtt_publisher.cpp

    src/storage/offline_store.cpp

    src/health/watchdog_manager.cpp

    src/common/config.cpp
    src/common/logger.cpp
    src/common/process_metrics.cpp
)

add_executable(tcp_device_simulator
    tools/tcp_device_simulator.cpp
)

add_executable(gatewayctl
    tools/gatewayctl.cpp
)

# Modbus设备模拟器目前只用于Ubuntu开发环境。
#
# 后续ModbusConnector完成以后，
# 再为Buildroot启用libmodbus并接入gatewayd。
if(NOT CMAKE_CROSSCOMPILING)
    add_executable(modbus_device_simulator
        tools/modbus_device_simulator.cpp
    )
    target_link_libraries(modbus_device_simulator
        PRIVATE
        PkgConfig::LIBMODBUS
    )
endif()

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
    PkgConfig::LIBMODBUS
)

target_link_libraries(tcp_device_simulator
    PRIVATE
    nlohmann_json::nlohmann_json
)