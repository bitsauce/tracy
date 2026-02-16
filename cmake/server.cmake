set(TRACY_COMMON_DIR ${CMAKE_CURRENT_LIST_DIR}/../public/common)

set(TRACY_COMMON_SOURCES
    tracy_lz4.cpp
    tracy_lz4hc.cpp
    TracySocket.cpp
    TracyStackFrames.cpp
    TracySystem.cpp
)

list(TRANSFORM TRACY_COMMON_SOURCES PREPEND "${TRACY_COMMON_DIR}/")


set(TRACY_SERVER_DIR ${CMAKE_CURRENT_LIST_DIR}/../server)

set(TRACY_SERVER_SOURCES
    TracyMemory.cpp
    TracyMmap.cpp
    TracyPrint.cpp
    TracySysUtil.cpp
    TracyTaskDispatch.cpp
    TracyTextureCompression.cpp
    TracyThreadCompress.cpp
    TracyWorker.cpp
)

list(TRANSFORM TRACY_SERVER_SOURCES PREPEND "${TRACY_SERVER_DIR}/")


add_library(TracyServer STATIC ${TRACY_COMMON_SOURCES} ${TRACY_SERVER_SOURCES})
target_include_directories(TracyServer PUBLIC ${TRACY_COMMON_DIR} ${TRACY_SERVER_DIR})
target_link_libraries(TracyServer PUBLIC TracyCapstone TracyZstd)
if(NO_STATISTICS)
    target_compile_definitions(TracyServer PUBLIC TRACY_NO_STATISTICS)
endif()

if(NOT NO_PARALLEL_STL AND UNIX AND NOT APPLE AND NOT EMSCRIPTEN)
    target_link_libraries(TracyServer PRIVATE TracyTbb)
endif()

if (ENABLE_WEBSOCKETS AND NOT EMSCRIPTEN)
    find_package(OpenSSL)
    if (${OpenSSL_FOUND})
        message(STATUS "OpenSSL found, secure web sockets will be supported")
        target_link_libraries(TracyServer PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        # PUBLIC so the profiler app can gate UI on whether secure WS is compiled in.
        target_compile_definitions(TracyServer PUBLIC ENABLE_SECURE_WEBSOCKETS)
    else ()
        message(STATUS "OpenSSL NOT found, secure web sockets will NOT be supported")
    endif ()

    CPMAddPackage(
        NAME websocketpp
        GIT_REPOSITORY https://github.com/zaphoyd/websocketpp
        GIT_TAG 0.8.2
        EXCLUDE_FROM_ALL TRUE
        OPTIONS
            "BUILD_TESTS OFF"
            "BUILD_EXAMPLES OFF"
    )
    CPMAddPackage(
        NAME asio
        GIT_REPOSITORY https://github.com/chriskohlhoff/asio
        GIT_TAG asio-1-32-0
        EXCLUDE_FROM_ALL TRUE
    )
    target_include_directories(TracyServer PRIVATE
        ${asio_SOURCE_DIR}/asio/include
        ${websocketpp_SOURCE_DIR}
    )
    # ENABLE_WEBSOCKETS is PUBLIC so the profiler app can gate UI on it; the rest
    # are websocketpp/asio implementation details that only TracyServer's TUs need.
    target_compile_definitions(TracyServer PUBLIC ENABLE_WEBSOCKETS)
    target_compile_definitions(TracyServer PRIVATE
        _WEBSOCKETPP_CPP11_STL_
        ASIO_STANDALONE
    )
endif ()
