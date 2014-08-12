context("Utilities work correctly")

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