cmake_minimum_required(VERSION 3.5.1)

project(dsp C CXX)

set(CMAKE_CXX_STANDARD 17)
include(cmake/common.cmake)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# Proto file
get_filename_component(hw_proto "dps.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/dps.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/dps.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/dps.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/dps.grpc.pb.h")
add_custom_command(
        OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "."
        --cpp_out "."
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories(".")

# hw_grpc_proto
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(publisher publisher.cc)
target_link_libraries(publisher
  hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(subscriber subscriber.cc)
target_link_libraries(subscriber
  hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(broker broker.cc timer.cc)
target_link_libraries(broker
  hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(guru guru.cc timer.cc)
target_link_libraries(guru
  hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# target_compile_options(db_client PUBLIC -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-unused-parameter)
# target_compile_options(db_client PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17 -D_FILE_OFFSET_BITS=64>)
target_compile_options(publisher PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17>)
target_compile_options(subscriber PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17>)
target_compile_options(broker PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17>)
target_compile_options(guru PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-std=c++17>)

