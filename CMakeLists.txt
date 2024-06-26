cmake_minimum_required(VERSION 3.2)
project(wing)
set(CMAKE_CXX_STANDARD 20)

# Set default build type to Release
IF (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
        "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
ENDIF()

option(SAKURA_ONLINE_JUDGE "The online judge will set this to ON" OFF)
option(NOT_USE_CPPTRACE "Whether not to enable cpptrace (off by default)" OFF)

# Submodules
add_subdirectory(third_party/fmt)
add_subdirectory(third_party/replxx)
if (NOT NOT_USE_CPPTRACE)
  add_subdirectory(third_party/cpptrace)
endif()

# Source files
file(GLOB DB_SOURCE
  src/catalog/*
  src/common/*
  src/execution/*
  src/execution/vec/*
  src/execution/volcano/*
  src/execution/predicate_transfer/*
  src/execution/*
  src/instance/*
  src/parser/*
  src/plan/*
  src/plan/rules/*
  src/plan/predicate_transfer/*
  src/storage/*
  src/storage/bplus_tree/*
  src/storage/lsm/*
  src/transaction/*
  src/type/*
)

# Configure LLVM
if (BUILD_JIT)
  find_package(LLVM REQUIRED CONFIG)
  message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
  message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
  set(LLVM_LINK_COMPONENTS
    Analysis
    Core
    ExecutionEngine
    InstCombine
    Object
    OrcJIT
    RuntimeDyld
    ScalarOpts
    Support
    TransformUtils
    native)
  llvm_map_components_to_libnames(llvm_libs ${LLVM_LINK_COMPONENTS})
  include_directories(${LLVM_INCLUDE_DIRS})
  add_definitions(${LLVM_DEFINITIONS})
  add_compile_definitions(BUILD_JIT=1)
  file(GLOB DB_JIT_SOURCE src/execution/jit/*)
  set(DB_SOURCE ${DB_SOURCE} ${DB_JIT_SOURCE})
endif()

add_compile_options("-Wall")
if (NOT WING_SANITIZER)
    set(WING_SANITIZER address)
endif ()
# Sanitizer can only be enabled in Debug mode
option(SAN "Use Sanitizer" OFF)
if (SAN)
  string( TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER )
  if (CMAKE_BUILD_TYPE_LOWER STREQUAL "debug")
    add_compile_options("-fsanitize=${WING_SANITIZER}" "-fno-omit-frame-pointer")
    add_link_options("-fsanitize=${WING_SANITIZER}" "-fno-omit-frame-pointer")
    message(STATUS "sanitizer is enabled.")
  else()
    message(STATUS "sanitizer will not be enabled in modes other than Debug.")
  endif()
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND MINGW)
  # https://stackoverflow.com/questions/31617034/stdfuture-and-clang-with-stdlib-libstdc
  # If you are using Clang but library was compiled by GCC
  add_compile_options("-femulated-tls")
endif()


add_library(wing_lib STATIC ${DB_SOURCE})
add_executable(${PROJECT_NAME} src/main.cpp)

if (MINGW)
  target_compile_options(wing_lib PRIVATE "-Wa,-mbig-obj")
endif()

# Include directories
target_include_directories(${PROJECT_NAME} PRIVATE src third_party/fmt)
target_include_directories(wing_lib PRIVATE src third_party/replxx/include third_party/fmt)
target_link_libraries(wing_lib PRIVATE ${llvm_libs} fmt replxx)
target_link_libraries(wing_lib PUBLIC pthread)
if (SAKURA_ONLINE_JUDGE)
  target_compile_definitions(wing_lib PUBLIC SAKURA_ONLINE_JUDGE)
endif()
if (NOT NOT_USE_CPPTRACE)
  target_compile_definitions(wing_lib PUBLIC USE_CPPTRACE=1)
  target_link_libraries(wing_lib PUBLIC cpptrace::cpptrace)
endif()
target_link_libraries(${PROJECT_NAME} PRIVATE wing_lib fmt)

# Test
add_subdirectory(test)

add_custom_target(submit
  zip ${CMAKE_CURRENT_BINARY_DIR}/submission.zip -r src third_party
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
