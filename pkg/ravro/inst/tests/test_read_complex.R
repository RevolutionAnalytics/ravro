# Copyright 2014 Revolution Analytics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#!/bin/bash
context("Complex example file")
complex.file.loc <- file.path(system.file("data",package="ravro"),
          "complex.avro")

test_that("flattening works", {
  complex <- read.avro(complex.file.loc,flatten=T)
  expect_that(dim(complex), equals(c(1,31)))
})

test_that("record -> df", {
  complex <- read.avro(complex.file.loc,flatten=F)
  expect_is(complex$data, "data.frame")
  expect_is(complex$algorithms, "data.frame")
})

test_that("map -> list", {
  complex <- read.avro(complex.file.loc,flatten=F)
  expect_is(complex$rules, "list")
})

test_that("unflattening works", {
  complex <- read.avro(complex.file.loc,flatten=F)
  expect_that(dim(complex), equals(c(1,16)))
  })
