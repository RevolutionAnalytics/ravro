# Copyright 2014 Revolution Analytics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#!/bin/bash

## List Transpose
## 
## Transpose a list of lists
## 
## @param l list
## 
## @examples
## 
## ravro:::t.list(mtcars)[[1]]
#' @import Rcpp
#' @useDynLib ravro
t.list  <- function(x) {
    if(length(x) == 0){
      x
    } else {
      .Call("t_list", x, PACKAGE = "ravro")
    }
}

## Takes a data.frame with potentially "nested" data.frame columns
## and flattens it
flatten <- function(x){
    structure(do.call(c,mapply(function(xj,xjname)
      # If it's a dataframe/Avro "record"
      # "flatten" it
      if(is.data.frame(xj)){
        xj <- flatten(xj)
        names(xj) <- 
          paste0(xjname,".",names(xj))
        # Return it as a list, otherwise it's created
        # as a single column containing a dataframe,
        # which is cool, but not desired
        unclass(xj)
      }else {
        # Make sure it's named correctly
        lst <- list(xj)
        names(lst) <- xjname
        lst
      },
      x,
      names(x),
      SIMPLIFY=F,
      USE.NAMES=F)),
      class="data.frame",row.names=attr(x,"row.names"))
}

## Takes a data.frame with "." names and unflattens it 
## so that it has nested data.frame columns
unflatten <- function(x){
  xnames <- names(x)
  unflat_index <- which(grepl(".",xnames,fixed=T))
  if (length(unflat_index)>0){
    unflat_names <- xnames[unflat_index]
    nested_record <- sapply(strsplit(unflat_names,".",fixed=T),`[[`,1L)
    record_splits <- split(unflat_index,nested_record)
    record_index <- sapply(record_splits,`[[`,1L) # grab the first occurence of each record
    other_index <- unlist(lapply(record_splits,`[`,-1L)) # grab the rest
    x[record_index] <- lapply(names(record_splits),function(record){
      original_names <- record_splits[[record]]
      nested_names <- substring(xnames[original_names],nchar(record)+2L)
      xi <- structure(x[original_names],
                names=nested_names,
                class="data.frame",
                row.names=attr(x,"row.names"))
      unflatten(xi)
    })
    names(x)[record_index] <- names(record_splits)
    x <- x[-other_index]
  }
  x
}

# wrap each element of x as a single-element list,
# using the corresponding element from names as the name for the single element 
enlist <- function(x,names){
  mapply(
    function(xi,name){
      xlst=list(xi)
      names(xlst) = name
      xlst
    },x,names,
    SIMPLIFY=F,
    USE.NAMES=F)
}