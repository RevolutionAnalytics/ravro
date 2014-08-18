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

#' Read and write Avro data files from R.
#'
#' The ravro package provides tools to allow reading of Avro data files into R.
#' It provides the functions \code{\link{read.avro}}
#' for reading Avro data files.
#'
#' @section Dependencies:
#' \subsection{R Packages}{
#' Prior to installing the ravro package, the following R packages must be
#' installed: Rcpp, rjson, bit64 and bit.  These can be installed in R with
#' the command:
#'
#' \code{install.packages(c("Rcpp","rjson","bit64"))}
#'
#' This command will install these packages, as well as the "bit" package,
#' which is required by bit64.
#' }
#'
#' \subsection{Java}{
#' In addition, Java must be installed on the system and be available via
#' the \code{PATH} environmental variable. Try running the following from R:
#'
#' \code{system("java -version")}
#'
#' The Java version must be at least 1.6
#' }
#'
#' @section Installation:
#' After these dependencies have been installed, ravro can be installed
#' from the command line:
#'
#' \code{R CMD INSTALL ravro_1.0.zip}
#'
#' Or through an R development environment, e.g. RStudio.
#'
#' You can verify that the package is working correctly on your system with
#'
#' \code{R CMD check ravro_1.0.zip}
#'
#' NOTE: On Linux and Mac OS systems, you will install and/or check the source
#' package: \code{ravro_1.0.tar.gz}.
#'
#' @references Apache Avro 1.7.6 Specification. \url{http://avro.apache.org/docs/1.7.6/spec.html}.
#' @import bit64 rjson
#' @docType package
#' @name ravro-package
NULL


sanitize_path <- function(path,cmdline=T){
  path <- suppressWarnings(normalizePath(path,winslash="/"))
  if(.Platform$OS.type == "unix") {
    gsub(" ","\\ ",path,fixed=TRUE)
  } else {
    paste0("\"",path,"\"")
  }

}

#' Location of the Java Avro Tools jar file
#'
#' This variable defines the path to the Java Avro Tools jar file that is used by the \code{ravro} package.
#'
#' This defines the location of the specific Avro Tools
#' jar file used internally to read the Avro schema for a data file and
#' to convert Avro data files to JSON.
#'
#' This value defaults to the bundled Avro Tools jar file contained within the \code{ravro}
#' package, but can be modified by defining the system environmental variable \code{AVRO_TOOLS}.
#'
#' @docType data
#' @format Location of the Java Avro Tools jar file
#' @name AVRO_TOOLS
#' @export
NULL
AVRO_TOOLS <- NULL
.onLoad  <- function(libname, pkgname){
  AVRO_TOOLS <<- sanitize_path(
  if (Sys.getenv("AVRO_TOOLS")!=""){
    Sys.getenv("AVRO_TOOLS")
  }else {
    file.path(system.file("java",package=pkgname),
              "avro-tools-1.7.4.jar")
  })
}
