cmake_minimum_required(VERSION 3.0)
project(API_CXX)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BUILD_DIR})
set(API_SRC ${CMAKE_CURRENT_SOURCE_DIR})

#===============================================================
if (WIN32)
    set(platform_macro "-DWINDOWS")
elseif (UNIX)
    set(platform_macro "-DLINUX -D_GLIBCXX_USE_CXX11_ABI=0")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${platform_macro} -fPIC  -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3")

#===============================================================
if (WIN32)
    link_directories(${API_SRC}/bin/mingw_x64)
elseif(UNIX)
    link_directories(${API_SRC}/bin/linux_x64)
endif ()
include_directories(${API_SRC}/include)

if (EXISTS "/usr/lib/libssl.so.1.0.0") # archlinux
    set(LIBSSL /usr/lib/libssl.so.1.0.0)
else ()
    set(LIBSSL ssl)
endif ()

if (WIN32)
    set(platform_lib "ws2_32")
elseif (UNIX)
    set(platform_lib ${LIBSSL})
endif ()

#===============================================================
macro(TEST module)
    add_executable(${module} ${API_SRC}/test/Streaming${module}.cpp)
    target_link_libraries(${module} 
        DolphinDBAPI 
        ${platform_lib} 
        pthread 
        uuid)
endmacro()

TEST(PollingClientTester)
TEST(ThreadedClientTester)
TEST(ThreadPooledClientTester)

