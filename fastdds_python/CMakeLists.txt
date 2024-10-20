# Copyright 2022 Proyectos y Sistemas de Mantenimiento SL (eProsima).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.22)

# SWIG: use standard target name.
if(POLICY CMP0078)
  cmake_policy(SET CMP0078 NEW)
endif()

# SWIG: use SWIG_MODULE_NAME property.
if(POLICY CMP0086)
  cmake_policy(SET CMP0086 NEW)
endif()

project(fastdds_python VERSION 1.4.2)

# Set BUILD_TESTING to OFF by default.
if(NOT BUILD_TESTING)
    message(STATUS "Tests not compiled by default")
    set(BUILD_TESTING OFF CACHE BOOL "Enable testing" FORCE)
endif()

###############################################################################
# Dependencies
###############################################################################

find_package(SWIG REQUIRED)
include(${SWIG_USE_FILE})
set(CMAKE_SWIG_FLAGS "")

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

find_package(fastcdr REQUIRED)
find_package(fastrtps 2.12 REQUIRED)

###############################################################################
# Project subdirectories
###############################################################################

# add_subdirectory(src/swig)

###############################################################################
# Pybind11 wrapper
###############################################################################

add_subdirectory(../../pybind11 build)

pybind11_add_module(fastdds_pybind
  NO_EXTRAS # TODO (AM): This flag is useful for debugging, but the binary grows to 3.2MB (vs 1.3MB) with this
  src/fastdds_pybind.cpp
)

target_include_directories(fastdds_pybind PRIVATE
  ${fastrtps_INCLUDE_DIR}
  ${fastcdr_INCLUDE_DIR}
)

target_link_libraries(fastdds_pybind PRIVATE
  fastrtps
  fastcdr 
)

install(TARGETS fastdds_pybind DESTINATION ${LIBRARY_RESOURCES_DIRECTORY_ABS})

###############################################################################
# Testing options
###############################################################################
enable_testing()
include(CTest)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
