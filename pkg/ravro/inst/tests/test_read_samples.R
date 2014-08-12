context("Read Example Avro Files")

test_that("yield reads unflattened correctly", {
  yield <- read.avro(file.path(system.file("data",package="ravro"),"yield1k.avro"),flatten=F)
  expect_true(all(sapply(yield$nonStandardData,ncol)==3))
  expect_true(all(sapply(yield$nonStandardData,nrow)==11))
  expect_is(yield, "data.frame")
  expect_equal(names(yield), c("header","record","nonStandardData"))
  expect_is(yield$header, "data.frame")
  expect_is(yield$nonStandardData, "list")
})

test_that("yield reads flattened correctly", {
  yield <- read.avro(file.path(system.file("data",package="ravro"),"yield1k.avro"),flatten=T)
  expect_is(yield, "data.frame")
  expect_equal(ncol(yield), 43)
  expect_true(all(sapply(yield$nonStandardData,ncol)==3))
  expect_true(all(sapply(yield$nonStandardData,nrow)==11))
  expect_true("header.field.boundary.id" %in% names(yield))
  expect_is(yield$record.timeStamp, "integer64")
  expect_is(yield$nonStandardData, "list")
})


test_that("as-planted reads unflattened correctly", {
  as_planted <- read.avro(file.path(system.file("data",package="ravro"),"as-planted1k.avro"),
                     flatten=F)
  expect_is(as_planted, "data.frame")
  expect_equal(names(as_planted), c("header","record","nonStandardData"))
  expect_is(as_planted$header, "data.frame")
  expect_true(is.na(as_planted$header$name[1]))
  # this should be NaN, but isn't because we can't create Avro NaN from JSON
  expect_true(is.na(as_planted$record$qualityMeasures$singulation$singulationPercentInstant[7]))
  expect_is(as_planted$nonStandardData, "list")
})

test_that("as-planted reads flattened correctly", {
  as_planted <- read.avro(file.path(system.file("data",package="ravro"),"as-planted1k.avro"),
                     flatten=T)
  expect_is(as_planted, "data.frame")
  expect_equal(ncol(as_planted), 66)
  expect_true(is.na(as_planted$header.name[1]))
  # this should be NaN, but isn't because we can't create Avro NaN from JSON
  expect_true(is.na(as_planted$record.qualityMeasures.singulation.singulationPercentInstant[7]))
  expect_is(as_planted$nonStandardData, "list")
})
