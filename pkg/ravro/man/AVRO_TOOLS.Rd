\docType{data}
\name{AVRO_TOOLS}
\alias{AVRO_TOOLS}
\title{Location of the Java Avro Tools jar file}
\format{Location of the Java Avro Tools jar file}
\description{
This variable defines the path to the Java Avro Tools jar
file that is used by the \code{ravro} package.
}
\details{
This defines the location of the specific Avro Tools jar
file used internally to read the Avro schema for a data
file and to convert Avro data files to JSON.

This value defaults to the bundled Avro Tools jar file
contained within the \code{ravro} package, but can be
modified by defining the system environmental variable
\code{AVRO_TOOLS}.
}

