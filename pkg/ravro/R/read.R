
#' Get Avro Schema
#' 
#' Get the schema that was used to write an Avro data file
#' 
#' Uses the \code{\link{AVRO_TOOLS}} jar file and the "getschema" command to retrieve the Avro 
#' schema for \code{file}.
#' 
#' @param file the path to an Avro data file 
#' 
#' @return
#' A list representation of the Avro JSON schema as parsed using \code{\link{fromJSON}}.
#' 
#' @examples
#' # Built-in example Avro example file containing the mtcars dataset
#' mtavro_path <- system.file("data/mtcars.avro",package="ravro")
#' str(avro_get_schema(mtavro_path))
#' # List of 3
#' # $ type  : chr "record"
#' # $ name  : chr "mtcars"
#' # $ fields:List of 12
#' # ..$ :List of 2
#' # .. ..$ name: chr "mpg"
#' # .. ..$ type: chr "double"
#' # ..$ :List of 2
#' # .. ..$ name: chr "cyl"
#' # .. ..$ type: chr "double"
#' ## etc.
#' 
#' @export
#' @references Apache Avro 1.7.6 Specification. \url{http://avro.apache.org/docs/1.7.6/spec.html}.
avro_get_schema <- function(file){
  # This will always work with stderr=F and stdout=T, in contrast to the reverse
  suppressWarnings({result <- system2("java",c("-jar",AVRO_TOOLS,"getschema",sanitize_path(file)),
                    stderr=F,stdout=T)})
  if (!is.null(attr(result,"status"))){
    #Error from avro-tools jar
    stop("Error retrieving schema.  Verify that the file exists and is a valid Avro: ",sanitize_path(file))
  }
  rjson::fromJSON(paste0(result,collapse=""))
}

## Convert Avro to JSON
## 
## Convert an Avro Data File to a JSON file
## 
## Uses the \code{\link{AVRO_TOOLS}} jar file and the "tojson" command to convert the Avro 
## input file into the output file.
## 
## @param input the path to an Avro data file
## @param output the path to a JSON output file
## 
## @return
## 
## Returns \code{output} character value.
## 
## @examples
## mtavro_path <- system.file("data/mtcars.avro",package="ravro")
## readLines(avro_to_json(mtavro_path),n=1)
## 
## @export
avro_to_json <- function(input,
                         output=tempfile(
                           pattern=basename(input),
                           fileext=".json")){
	err <- {
		if(.Platform$OS.type == "windows") 
			system2("java",c("-jar",AVRO_TOOLS,"tojson",sanitize_path(input)),stdout=output,
          stderr=T)
		else
			system(paste( "java","-jar",AVRO_TOOLS,"tojson",sanitize_path(input), ">", output), intern = TRUE)}
  if (length(err)>0){
    message(err)
    stop("Error Avro to JSON")
  }
  invisible(output)
}

# Convert a parsed JSON "record" dataset into R
# @param x a list of Avro "record" datum values as parsed JSON
# @param schema the Avro schema for this record parsed from JSON into R
parse_avro.record <- function(x,schema,flatten=T,...){
  # Convert from row-based list to column-based
  is_empty <- F
  if (is.null(x)){
    x <- NA
    is_empty <- T
  }
  nc <- length(schema$fields)
  nr <- length(x)
  # Just replace NULL with a row of NA
  x <- fill.with.NAs.if.short(x, nc)
  x_df <- t.list(x)
  row_names <- .set_row_names(nr)
  
  is_row_name <- names(x_df) == "row_names"
  if (any(is_row_name)){
    row_names <- parse_avro(x_df$row_names,schema$fields[[which(is_row_name)]])
    x_df$row_names <- NULL
    schema$fields <- schema$fields[!is_row_name]
  }
  # For each field, "parse" it into R
  x_df <- structure(# Force it to a dataframe without any checking
    if (flatten){
      do.call(c, # "flatten" any "record" fields
              lapply(seq_along(x_df),
                     function(field){
                       parsed_field <- parse_avro(x_df[[field]],schema$fields[[field]],
                                                  flatten=flatten,...)
                       # If it's a dataframe/Avro "record"
                       # "flatten" it
                       lst <- if (is.data.frame(parsed_field)){
                         names(parsed_field) <- 
                           paste0(schema$fields[[field]]$name,".",names(parsed_field))
                         # Return it as a list, otherwise it's created
                         # as a single column containing a dataframe,
                         # which is cool, but not desired
                         unclass(parsed_field)
                       }else {
                         # Make sure it's named correctly
                         lst <- list(parsed_field)
                         names(lst) <- schema$fields[[field]]$name
                         lst
                       }
                       lst
                     }))
    }else {
      fields <- mapply(parse_avro,x_df,schema$fields,SIMPLIFY=F,
                       flatten=flatten,...)
      names(fields) <- sapply(schema$fields,`[[`,"name")
      fields
    },
    class="data.frame",row.names=row_names)
  if (is_empty){
    x_df[-1,]
  }else {
    x_df
  }
}

parse_avro.array <- function(x,schema,simplify=F,encoded_unions=T,...){
  schema_data <- parse_avro_schema(schema$items)
  if (schema_data$is_union && encoded_unions){ 
    x <- lapply(x,function(xi){
        xi <- unlist(xi,recursive=F)
        names(xi)=NULL
        xi
    })
    names(x) <- NULL
  }
    # "parse" each value, since each value is itself an "array"/list
    sapply(x,parse_avro,schema=schema_data$schema,simplify=simplify,
           encoded_unions=encoded_unions,...)
}

parse_avro.enum <- function(x,schema,...){
  x <- factor(unlist(x),levels=schema$symbols)
  # This should be safer and faster than converting the values directly
  levels(x) <- ravro_get_symbol_level(levels(x))
  x
}

## Function for grabbing key details of the schema
## Particularly for the somewhat awkward way in which fields must be defined
parse_avro_schema <- function(schema){
    if (is.character(schema)){
      type <- schema
      schema <- list(type=type)
    }else {
      type <- schema$type
    }
  is_union=F
  if (is.list(type)){ # not a primitive
    if ("type" %in% names(type)){# Nested, probably record, schema object
      schema <- type
      type <- schema$type
    }else {# Union type
      is_union=T
      type <- type[type!="null"]
      # Nulls are automatically converted to NA inside parse_avro.record
      if (length(type)>1){
        stop("Unions of multiple non-null types not allowed for field ",schema$name)
      }
      schema <- type[[1]]
      type <- schema$type
    }
  }else if (length(type)>1){
    type <- type[type!="null"]
    is_union <- T
    if (length(type)>1){
      stop("Unions of multiple non-null types not allowed for field ",schema$name)
    }
  }
  list(schema=schema,type=type,is_union=is_union)
}


## Convert Avro JSON to R
## 
## Convert a parsed JSON dataset into R using the specified Avro schema.
## 
## Convert imported Avro data into useful R objects using the Avro schema.  Both the data, 
## \code{x}, and the schema, \code{schema}, are assumed to have been parsed from \code{JSON}
## using \code{fromJSON}.
## 
## 
## @param x a list of Avro datum values as parsed JSON
## @param schema the Avro schema for this value parsed from JSON into R
## @param encoded_unions are unions encoded with their fully qualified type according to the 
## Avro JSON encoding specification or are unions simply encoded as their contents?
## 
## 
## @inheritParams read.avro
## @seealso \code{\link{fromJSON}}, \code{\link{integer64}}
## @references Apache Avro 1.7.6 Specification. \url{http://avro.apache.org/docs/1.7.6/spec.html}.
#' @import bit64
parse_avro <- function(x,schema,flatten=T,simplify=F,encoded_unions=T){
  schema_data <- parse_avro_schema(schema)
  schema <- schema_data$schema
  xtype <- schema_data$type
  
  # Deal with null for all data here, whether or not they're supposed to exist
  default = if(is.null(schema$default)) NA else schema$default
  x <- default.if.null(x, default)

  if (schema_data$is_union && encoded_unions){
    # The Avro JSON encoding encodes unions with an additional embedded JSON object
    x <- unlist(x,recursive=F)
  }
  if (!is.character(xtype) || length(xtype)>1){
    stop("Invalid type: ",paste0(xtype,collapse=", "))
  }
  switch(xtype,
         record=parse_avro.record(x,schema,flatten=flatten,simplify=simplify, 
                                  encoded_unions = encoded_unions),
         array=parse_avro.array(x,schema,flatten=flatten,simplify=simplify,
                                encoded_unions=encoded_unions),
         float=as.numeric(unlist(x)),
         double=as.numeric(unlist(x)),
         long=bit64::as.integer64(unlist(x)),
         int=as.integer(unlist(x)),
         boolean=as.logical(unlist(x)),
         string=as.character(unlist(x)),
         bytes=as.character(unlist(x)),
         fixed=as.character(unlist(x)),
         enum=parse_avro.enum(x,schema,flatten=flatten,simplify=simplify,
                              encoded_unions=encoded_unions),
         map=x,
         stop("Unsupported Avro type: ",xtype))
  
}

#' Avro Data Input
#' 
#' Reads a file in the Avro format and creates an appropriate R data value from it corresponding to the Avro schema used to create the file.
#' 
#' Reads an Avro data file into R in a four-step process:
#' \enumerate{
#' \item Retrieve the Avro schema used to write the Avro data file
#' \item Convert the Avro data to a \code{JSON} file using the Java Avro Tools
#' \item Read the \code{JSON} data into R and parse it using \code{\link{fromJSON}}
#' \item Using the schema, complete any additional transformations of the data to 
#' compatible and useful R objects}
#' 
#' Steps 3 and 4 are repeated, processing \code{buffer.length} Avro datum elements at a time
#' until either \code{n} records have been processed or the end of the file is reached.
#' 
#' The specific Avro Tools jar file is defined by \code{\link{AVRO_TOOLS}}
#' 
#' 
#' 
#' 
#' @param file path to an Avro data file
#' @param n the maximum number of Avro datums to read
#' @param buffer.length the (maximum) number of records to import at a time for conversion to
#' R objects
#' @param flatten combine all logical "record" fields into a single top-level dataframe
#' @param simplify logical or character string; should the result be simplified to a vector, 
#' matrix or higher dimensional array if possible? The default value, TRUE, returns a vector 
#' or matrix if appropriate. For more details, see \code{\link{sapply}}.
#' 
#' 
#' @return
#' 
#' Avro types will be converted to R object with the following mapping:
#' 
#' \itemize{
#' \item \code{null} -> R's \code{NA} value
#' \item \code{boolean} -> \code{logical}
#' \item \code{int}` -> \code{integer}
#' \item \code{long} -> \code{integer64} (from the `bit64` package)
#' \item \code{float},\code{double} -> \code{numeric}
#' \item \code{bytes},\code{fixed} -> \code{character} (\code{charToRaw} allows conversion to vector of type \code{raw}) 
#' \item \code{string} -> \code{character}
#' \item \code{record} -> \code{data.frame} (see below)
#' \item \code{enum} -> \code{factor}
#' \item \code{array} -> \code{list}
#' \item \code{map} ->  named \code{list}
#' \item \code{union} -> \code{list} or \code{vector}
#' }
#' 
#' In addition to this type mapping, the specific data structure is determined by 
#' the options \code{flatten} and \code{simplify}.  The \code{simplify} option causes 
#' \code{array} elements to be simplified in the same way that \code{\link{sapply}} results
#' are.
#' 
#' The \code{flatten} option causes nested \code{record} elements to be "lifted" up to the 
#' top-level dataframe value.  For example, the \code{\link{iris}} dataset could be stored as a 
#' top-level "iris" record containing "Sepal" and "Petal" \code{record} fields and a "Species"
#' \code{string} field.  When \code{flatten=TRUE}, this Avro dataset would be imported with the
#' same structure as the \code{\link{iris}} dataset.  Alternatively, \code{flatten=FALSE} would
#' import the same Avro dataset as a dataframe containing three columns, "Sepal" and "Petal" columns
#' that are themselves dataframes, and a "Species" column containing \code{character} values.  
#' This serialization of the \code{\link{iris}} dataset is stored as \code{data/iris.avro}.
#' 
#' For Avro \code{record} types, the \code{row.names} attribute is retrieved from the "row_names"
#' field, if such a field exists.
#' 
#' @examples
#' # Built-in example Avro example file containing the mtcars dataset
#' mtavro_path <- system.file("data/mtcars.avro",package="ravro")
#' 
#' # Read in the data
#' mtavro <- read.avro(mtavro_path)
#' names(mtavro)
#' # [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear" "carb"
#' all.equal(mtcars,mtavro)
#' # [1] TRUE
#' 
#' # Inspect the Avro schema
#' str(avro_get_schema(mtavro_path))
#' 
#' # Built-in example Avro example file containing the iris dataset
#' iris_avro_path <- system.file("data/iris.avro",package="ravro")
#' 
#' # Importing flattened data
#' str(read.avro(iris_avro_path))
#' #'data.frame':  150 obs. of  5 variables:
#' #  $ Sepal.Length: num  5.1 4.9 4.7 4.6 5 5.4 4.6 5 4.4 4.9 ...
#' #  $ Sepal.Width : num  3.5 3 3.2 3.1 3.6 3.9 3.4 3.4 2.9 3.1 ...
#' #  $ Petal.Length: num  1.4 1.4 1.3 1.5 1.4 1.7 1.4 1.5 1.4 1.5 ...
#' #  $ Petal.Width : num  0.2 0.2 0.2 0.2 0.2 0.4 0.3 0.2 0.2 0.1 ...
#' #  $ Species     : Factor w/ 3 levels "setosa","versicolor",..: 1 1 1 1 1 1 1 1 1 1 ...
#'  
#' # Importing unflattened data
#' str(read.avro(iris_avro_path,flatten=FALSE))
#' #'data.frame':  150 obs. of  3 variables:
#' # $ Sepal  :'data.frame':	150 obs. of  2 variables:
#' #   ..$ Length: num  5.1 4.9 4.7 4.6 5 5.4 4.6 5 4.4 4.9 ...
#' # ..$ Width : num  3.5 3 3.2 3.1 3.6 3.9 3.4 3.4 2.9 3.1 ...
#' # $ Petal  :'data.frame':	150 obs. of  2 variables:
#' #   ..$ Length: num  1.4 1.4 1.3 1.5 1.4 1.7 1.4 1.5 1.4 1.5 ...
#' # ..$ Width : num  0.2 0.2 0.2 0.2 0.2 0.4 0.3 0.2 0.2 0.1 ...
#' # $ Species: Factor w/ 3 levels "setosa","versicolor",..: 1 1 1 1 1 1 1 1 1 1 ...
#' 
#' @export
#' @import rjson
#' @seealso \code{\link{integer64}}, \code{\link{AVRO_TOOLS}}
#' @references Apache Avro 1.7.6 Specification. \url{http://avro.apache.org/docs/1.7.6/spec.html}.
read.avro <- function(file,n=-1L,flatten=T,simplify=F,buffer.length=10000){
  schema <- avro_get_schema(file)
  file_json <- avro_to_json(file)
  con <- file(file_json,open="rt")
  on.exit(close(con))
  x <- NULL
  x_n <- 0;
  n <- if (n == -1L){
    Inf
  }else {
    n
  }
  while(TRUE){
    buffer <- readLines(con,n=buffer.length)
    if (length(buffer)==0){
      break
    }
    x_n <- x_n + length(buffer)
    # don't parse extra data
    if (x_n > n){
      buffer <- head(buffer,n=-(x_n - n))
    }
    buffer_x <- parse_avro(
      fromJSON(paste0("[",paste0(buffer,collapse=","),"]")),
      schema=schema,flatten=T,simplify=simplify)
    
    if (is.null(x)){
      x <- buffer_x
    }else {
      if (is.data.frame(x)){
        x <- rbind(x,buffer_x)
      }else {
        x <- c(x,buffer_x)
      }
    }
  }
  if (flatten){
    x
  }else {
    unflatten(x)
  }
}
