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
# limitations under the License.

context("Basic Avro Read/Write")

test_that("Handling factors", {
  # Factors with non-"name" levels should still work
  d <- data.frame(x = 1,
                  y = as.factor(1:10),
                  fac = as.factor(sample(letters[1:3], 10, replace = TRUE)))
  d_avro <- tempfile(fileext=".avro")
  expect_warning(ravro:::write.avro(d, d_avro))
  expect_equal(read.avro(d_avro),d)
})

test_that("type translation", {
  # All types should translate successfully
  L3 <- LETTERS[1:3]
  fac <- sample(L3, 10, replace = TRUE)
  d <- data.frame(x = 1, y = 1:10, fac = fac, b = rep(c(TRUE, FALSE),5), c = rep(NA, 10),
                  stringsAsFactors=FALSE)
  d_avro <- tempfile(fileext=".avro")
  expect_true(ravro:::write.avro(d, d_avro,"d"))

  d.sch <- avro_get_schema(d_avro)
  expect_that(d.sch$type, equals("record"))
  expect_that(d.sch$name, equals("d"))
  expect_that(d.sch$fields[3][[1]]$type, equals("string"))

  expect_equal(read.avro(d_avro),d)


  d <- data.frame(x = 1, y = 1:10, fac = factor(fac,levels=L3),
                  b = rep(c(TRUE, FALSE),5), c = rep(NA, 10),
                  stringsAsFactors=FALSE)

  d_avro <- tempfile(fileext=".avro")

  expect_true(ravro:::write.avro(d, d_avro,"d"))

  expect_equal(read.avro(d_avro),d)

  d.sch <- avro_get_schema(d_avro)
  expect_that(d.sch$type, equals("record"))
  expect_that(d.sch$name, equals("d"))
  expect_that(d.sch$fields[1][[1]]$type, equals("double"))
  expect_that(d.sch$fields[2][[1]]$type, equals("int"))
  expect_that(d.sch$fields[3][[1]]$type$type, equals("enum"))
  expect_that(d.sch$fields[3][[1]]$type$symbols, equals(L3))
  expect_that(d.sch$fields[4][[1]]$type, equals("boolean"))
  expect_that(d.sch$fields[5][[1]]$type, equals(c("null","boolean")))

})

context("Basic Avro Write")
test_that("write.avro accepts multiple types", {
  d <- 1
  expect_true(ravro:::write.avro(d,d_avro <- tempfile(fileext=".avro")))
  expect_equal(read.avro(d_avro),d)
  d <- 1:5
  expect_true(ravro:::write.avro(d,d_avro <- tempfile(fileext=".avro")))
  expect_equal(read.avro(d_avro),d)
  d <- as.list(1:5)
  expect_true(ravro:::write.avro(d,d_avro <- tempfile(fileext=".avro")))
  expect_equal(read.avro(d_avro),d)
  expect_that(ravro:::write.avro(matrix(1:40,nrow=4),
                                 d_avro <- tempfile(fileext=".avro")),throws_error())
  expect_that(ravro:::write.avro(structure(as.list(letters),names=letters),
                                 d_avro <- tempfile(fileext=".avro")),gives_warning())
})

test_that("write can handle missing values", {
  # NA column (entirely "null" in Avro)
  d <- data.frame(x = 1,
                  y = 1:10,
                  b = rep(c(TRUE, FALSE),5),
                  c = rep(NA, 10),
                  stringsAsFactors=FALSE)
  d_avro <- tempfile(fileext=".avro")
  expect_true(ravro:::write.avro(d, d_avro))
  expect_equal(read.avro(d_avro),d)

  # NA row (entirely "null" in Avro)
  d <- rbind(data.frame(x = 1,
                        y = 1:10,
                        b = rep(c(TRUE, FALSE),5)),
             rep(NA,3))
  d_avro <- tempfile(fileext=".avro")
  expect_true(ravro:::write.avro(d, d_avro))
  expect_equal(read.avro(d_avro),d)
})


test_that("NaNs throw warning", {
  # NaN row (entirely "null" in Avro)
  d <- rbind(data.frame(x = 1,
                        y = 1:10,
                        b = rep(c(TRUE, FALSE),5)),
             rep(NaN,3))
  d_avro <- tempfile(fileext=".avro")
  expect_that(ravro:::write.avro(d, d_avro), gives_warning())
  d[nrow(d),] <- NA
  expect_equal(read.avro(d_avro),d)

  # NaN row (entirely "null" in Avro)
  d <- cbind(data.frame(x = 1,
                        y = 1:10,
                        b = rep(c(TRUE, FALSE),5)),
             c=rep(NaN,10))
  d_avro <- tempfile(fileext=".avro")
  expect_that(ravro:::write.avro(d, d_avro), gives_warning())
  d[,ncol(d)] <- as.numeric(NA) # coerce this type
  expect_equal(read.avro(d_avro),d)
})


test_that("write.avro throws error on infinite values", {
  d <- rbind(data.frame(x = 1, y = 1:10, b = rep(c(TRUE, FALSE),5)), rep(NA,3),
             c(Inf, 11, TRUE, NA))
  expect_that(ravro:::write.avro(d, tempfile(fileext=".avro"),"d"), throws_error())
  d <- rbind(data.frame(x = 1, y = 1:10, b = rep(c(TRUE, FALSE),5)), rep(NA,3),
             c(-Inf, 11, TRUE, NA))
  expect_that(ravro:::write.avro(d, tempfile(fileext=".avro"),"d"), throws_error())

})

context("Read/Write mtcars and iris")
test_that("mtcars round trip", {
  mtcars_avro <- tempfile()
  expect_true(ravro:::write.avro(mtcars,file=mtcars_avro,name="mtcars"))
  expect_that(read.avro(mtcars_avro), equals(mtcars))
  mtschema <- ravro:::avro_make_schema(mtcars,name="mtcars")
  expect_equal(avro_get_schema(mtcars_avro)[names(mtschema)],mtschema)
})

test_that("factors level that are not Avro names read/write", {
  mtcars_avro <- tempfile()
  mttmp <- mtcars
  mttmp$gear_factor <- as.factor(mttmp$gear)
  expect_true(ravro:::write.avro(mttmp,file=mtcars_avro,name="mtcars"))
  expect_that(read.avro(mtcars_avro), equals(mttmp))
})


test_that("iris round trip", {
  # This R structure should produce an un-flattened Avro schema
  iris_avro <- with(iris,
                    structure(list(Sepal=data.frame(Length=Sepal.Length,Width=Sepal.Width),
                                   Petal=data.frame(Length=Petal.Length,Width=Petal.Width),
                                   Species=Species),
                              class="data.frame",
                              row.names=attr(iris,"row.names")))
  iris_file <- tempfile()
  expect_true(ravro:::write.avro(iris_avro,file=iris_file,name="iris"))
  expect_that(read.avro(iris_file,buffer.length=10), equals(iris))
  expect_that(names(read.avro(iris_file,flatten=FALSE,buffer.length=10)),
              equals(c("Sepal","Petal","Species")))
})


