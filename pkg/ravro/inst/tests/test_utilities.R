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
# limitations under the License.#!/bin/bashcontext("Utilities work correctly")

test_that("t.list works", {
  # First column is the first transposed row
  expect_equivalent(ravro:::t.list(mtcars)[[1]],mtcars[1,])
})

test_that("unflatten iris works",{
  unflat_iris <- with(iris,
                    structure(list(Sepal=data.frame(Length=Sepal.Length,Width=Sepal.Width),
                                   Petal=data.frame(Length=Petal.Length,Width=Petal.Width),
                                   Species=Species),
                              class="data.frame",
                              row.names=attr(iris,"row.names")))
  expect_equal(ravro:::unflatten(iris),unflat_iris)
})

test_that("enlist letters works",{
  enlisted_letters  <- lapply(letters,function(x)structure(list(x),names="letter"))
  expect_equal(ravro:::enlist(letters,"letter"),enlisted_letters)

})
