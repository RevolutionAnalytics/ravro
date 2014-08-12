
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
