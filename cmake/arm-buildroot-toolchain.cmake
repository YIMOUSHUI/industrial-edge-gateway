set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(BUILDROOT_DIR
    "/home/dda/linux/tool/buildroot-2025.02.17"
)

set(CMAKE_C_COMPILER
    "${BUILDROOT_DIR}/output/host/bin/arm-buildroot-linux-gnueabihf-gcc"
)

set(CMAKE_CXX_COMPILER
    "${BUILDROOT_DIR}/output/host/bin/arm-buildroot-linux-gnueabihf-g++"
)

# 让CMake的find_package/find_library/find_path
# 优先在Buildroot的ARM sysroot中寻找，
# 而不是误用Ubuntu本机的x86_64库。
set(CMAKE_FIND_ROOT_PATH
    "${BUILDROOT_DIR}/output/staging"
)

# 编译过程中需要执行的工具仍然使用Ubuntu本机程序。
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)

# 头文件、库、CMake Package必须从ARM环境寻找。
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

# 交叉编译时pkg-config必须读取Buildroot staging中的ARM包信息，
# 不能读取Ubuntu宿主机的x86_64 .pc文件。
set(PKG_CONFIG_EXECUTABLE
    "${BUILDROOT_DIR}/output/host/bin/pkg-config"
    CACHE FILEPATH
    "Buildroot pkg-config"
    FORCE
)

set(
    ENV{PKG_CONFIG_SYSROOT_DIR}
    "${BUILDROOT_DIR}/output/staging"
)

set(
    ENV{PKG_CONFIG_LIBDIR}
    "${BUILDROOT_DIR}/output/staging/usr/lib/pkgconfig:${BUILDROOT_DIR}/output/staging/usr/share/pkgconfig"
)