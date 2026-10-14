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

    src/rules/rule_engine.cpp
    
    src/anomaly/anomaly_detector.cpp

    src/ai/rknn_inference_engine.cpp
    src/ai/ai_anomaly_detector.cpp
    
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

# ------------------------------------------------------------
# M4 RK3576 RKNN Runtime
#
# Ubuntu开发机：
#   通常找不到rknn_api.h / librknnrt.so，
#   此时gatewayd编译为无RKNN后端版本。
#
# RK3576：
#   librknnrt.so已经安装到系统；
#   rknn_api.h使用此前验证官方Demo时的SDK路径。
# ------------------------------------------------------------
find_path(
    RKNN_API_INCLUDE_DIR
    rknn_api.h

    HINTS
    /home/cat/rknn-m4-lab/rknpu2/runtime/Linux/librknn_api/include
)

find_library(
    RKNNRT_LIBRARY
    rknnrt
)

if(RKNN_API_INCLUDE_DIR AND RKNNRT_LIBRARY)

    message(
        STATUS
        "RKNN Runtime enabled: ${RKNNRT_LIBRARY}"
    )

    target_compile_definitions(
        gatewayd
        PRIVATE
        GATEWAY_HAS_RKNN=1
    )

    target_include_directories(
        gatewayd
        PRIVATE
        ${RKNN_API_INCLUDE_DIR}
    )

    target_link_libraries(
        gatewayd
        PRIVATE
        ${RKNNRT_LIBRARY}
    )

else()
    message(
        STATUS
        "RKNN Runtime not found; "
        "gatewayd will build without RKNN backend"
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