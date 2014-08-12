# does this avro type require a name
# @param xtype character value
avro_type_requires_name <- function(xtype){
  if (is.character(xtype)){
    xtype %in% c("record","fixed","enum")
  }else {
    avro_type_requires_name(xtype$type)
  }
}

ravro_make_symbol <- function(x)paste0("_",x,"_ravro")
is_ravro_symbol <- function(x)grepl("_[[:alnum:]_]*_ravro",x)
ravro_get_symbol_level <- function(x){
  ifelse(is_ravro_symbol(x),
         substring(x,2,nchar(x)-6),
         x)
}

is_avro_name <- function(x)grepl("^[[:alpha:]_][[:alnum:]_]*",x)

# is an Avro type convertible to an R atomic type
# @param xtype character value
avro_type_is_atomic <- function(xtype){
  if (is.null(xtype)){
    FALSE
  }else if (is.list(xtype)){
    if (!is.null(names(xtype))){
      avro_type_is_atomic(xtype$type)
    }else {
      all(sapply(xtype,avro_type_is_atomic))
    }
  }else if (is.character(xtype)){
    xtype %in% c("boolean","int","long","float","double",
                 "string","enum","fixed","bytes","null")  
  }else {
    FALSE
  }
}

# is an Avro type convertible to an R atomic type
# @param xtype character value
avro_convert_atomic <- function(x,xtype){
  to_raw_char <- function(x){
    if (is.character(x)){
      x
    }else if (is.raw(x)){
      sapply(x,rawToChar)
    }else{
      sapply(as.raw(x),rawToChar)
    }
  }
  switch(xtype,
         boolean=as.logical(x),
         int=as.integer(x),
         long=as.integer64(x),
         float=as.numeric(x),
         double=as.numeric(x),
         string=as.character(x),
         enum=x,
         fixed=as.character(x),
         bytes=as.character(x),
         null=NA)
}



# Get the basic Avro type for an R value
# 
avro_make_type <- function(x){
  if (is.atomic(x)){
    switch(class(x),
           logical="boolean",
           integer="int",
           integer64="long",
           numeric="double",
           character="string",
           factor="enum",
           NULL="null",
           stop("Unsupported atomic type: ",class(x)))
  }else if(is.list(x)){
    if (is.data.frame(x)){
      "record"
    }else{
      if (!is.null(names(x))){
        # There is no compatible Avro construct for data elements indexed by keys
        # (as opposed to data elements containing key-value pairs)
        warning("Non-null names attribute for list datum: ",paste(names(x),collapse=", "))
      }
      
      if (all(sapply(x,function(xi){!is.data.frame(xi) && 
                                      !is.null(names(xi)) && 
                                      all(!is.na(names(xi)))}))){
        # if the values are not data frames and they are list/vectors with names
        "map"
      }else if (all(sapply(x,function(xi)is.data.frame(xi) || is.null(names(xi))))){
        "array"
      }else {
        # This could lead to duplicate names and ambiguity as to whether
        # something is supposed to be a map or a array
        stop("Cannot create schema for named lists with NA names.")
      }
    }
  }else {
    stop("Invalid type: ",class(x))
  }
}

## Create Avro Schema
## 
## Create an Avro schema object for an R value
## 
## Create an Avro schema and convert an R \code{data.frame} value into a list of data elements.
## 
## Optional \code{name} and \code{namespace} arguments can be used to specify the top-level Avro 
## element \code{name} and \code{namespace}.
## 
## If \code{is.union = TRUE}, each data element will be wrapped in a \code{list} named with the 
## Avro type for that data element, consistent with the Avro JSON encoding.
## 
## If \code{is.field = TRUE}, slight modifications will be made to the Avro schema and data 
## elements, allowing them to be used as values/schemas for a Avro \code{record}'s "field" value.
## 
## 
## @param x data object for which to create an Avro schema
## @param name (optional)a "name" value for the top-level Avro schema element
## @param namespace (optional)a "namespace" value for the top-level Avro schema element
## @param row.names should the \code{row.names} attribute be exported as the field \code{row_names}.
## 
## @export
avro_make_schema <- function(x,name=NULL,namespace=NULL,
                             unflatten=F,
                             row.names=T){
  avro_make_data(x=x,name=name,
                 namespace=namespace,
                 is.union=F,
                 unflatten=unflatten,
                 row.names=row.names,
                 is.field=F)$schema
}

## Prepare data and schema for export to Avro
##
## @param is.union should the value be wrapped as a list and named with the type
## @param is.field should the value and schema be prepared as record fields
##
## @return
## A list with "schema" and "data" fields containing the formatted data and schema 
avro_make_data <- function(x,name=NULL,namespace=NULL,
                           is.union=F,
                           row.names=T,
                           unflatten=F,
                           is.field=F){
  schema <- list()
  xavro <- x
  xtype <- avro_make_type(x)
  
  if (!is.null(name)){
    schema$name <- name
  }
  
  if (avro_type_requires_name(xtype) && is.null(name)){
    stop("Avro type \"",xtype,"\" must be named.")
  }
  
  if (xtype =="null"){
    schema$type=xtype
  }else if (avro_type_is_atomic(xtype)){
    xname <- xtype
    if (xtype == "enum"){
      if (is.null(name)){
        stop("Avro type \"",xtype,"\" must be named.")
      }
      
      # include the namespace if it's not null
      xname <- if (!is.null(namespace)){
        paste0(namespace,".",name)
      }else {
        name
      }
      xtype <- list(name=name,type="enum")
      xavro <- as.factor(xavro)
      xtype$symbols <- levels(x)
    
      # Make sure user-defined values won't collide with ravro-generated symbols
      if (any(is_ravro_symbol(xtype$symbols))){
        stop("ravro uses the symbol namespace \"_*_ravro\" for auto-generated Avro symbol names")
      }
      invalid_names <- !is_avro_name(xtype$symbols)
      if (any(invalid_names)){
        # update invalid symbol levels, so we can grab character values
        # This should be safer and faster than converting the values directly
        xtype$symbols[invalid_names] <- 
          levels(xavro)[invalid_names] <- 
          ravro_make_symbol(xtype$symbols[invalid_names])
        warning("Factor levels converted to valid Avro names: ",
             paste0(xtype$symbols[invalid_names],collapse=", "))
      }
      xavro <- as.character(xavro)
    }
    if (any(is.infinite(xavro))){
      stop("Infinite values cannot be serialied to Avro")
    }
    
    
    schema$type <- xtype
    if (any(is.na(xavro)) || is.union){
      schema$type <- c(list("null"),list(xtype))
      if (any(is.nan(xavro))){
        warning("Converting NaN values to NA for Avro JSON")
      }
      x_na <- is.na(xavro) # Note: this finds NaN or NA
      xavro <- enlist(xavro,rep(xname,length(xavro)))
      xavro[x_na] <- list(NULL)
    }
  }else  if (xtype == "record"){    
    if (row.names){
      x$row_names <- attr(x,"row.names")
    }
    schema$type <- "record"
    # type information for fields should be embedded in the "fields" list
    xfields <- mapply(avro_make_data,x,name=names(x),
                      MoreArgs=list(namespace=namespace,
                                    is.field=T,
                                    unflatten=unflatten,
                                    row.names=row.names),SIMPLIFY=F)
    schema$fields <- lapply(xfields,`[[`,"schema")
    names(schema$fields) <- NULL # remove the names
    xavro <- t.list(lapply(xfields,`[[`,"data"))
    if (is.union){
      # In order to support unions, we need to embed the type, so each row needs to be a 
      # list containing a single value, with name equal to the record name
      xnames <- rep(
        if(is.null(namespace)){
          name
        }else{
          paste0(namespace,".",name)
        },
        length(xavro))
      
      xavro <- enlist(xavro,xnames)
    }else {
      names(xavro) <- NULL
    }
    if (is.field){
      schema <- list(name=name,type=schema)
    }
  }else if (xtype == "map" || xtype == "array"){
    schema$type <- xtype
    # Get a union of all types contained in the list of lists
    # Add "null", since list elements can be NULL
    x_avro <- lapply(x,function(xi){
      if (is.data.frame(xi)){
        if (is.null(name)){
          stop("Avro type \"",xtype,"\" must be named.")
        }
        val <- avro_make_data(xi,name=name,
                              namespace=namespace,
                              is.union=T,
                              unflatten=unflatten,
                              row.names=row.names)
        xi_data = val$data
        if (xtype == "map"){
          names(xi_data) <- row.names(xi)
        }else {#array
          names(xi_data) <- NULL
        }
        list(schema=list(val$schema),data=xi_data)
      }else {
        xi_avro <- lapply(xi,function(xij){
          xij_avro <- avro_make_data(xij,
                                     name=name,
                                     namespace=namespace,
                                     is.union=T,
                                     unflatten=unflatten,
                                     row.names=row.names)
          xij_data <- xij_avro$data
          xij_type <- if (all(avro_type_is_atomic(xij_avro$schema$type)) &&
                            length(xij_data)==1){
            xij_data <- xij_data[[1]]
            xij_avro$schema$type
          }else {
            list(xij_avro$schema)
          }
          list(schema=xij_type,data=xij_data)
        })
        list(schema=unlist(lapply(xi_avro,`[[`,"schema"),recursive=F),
             data=lapply(xi_avro,`[[`,"data"))
      }
    })
    value_types <- unique(unlist(lapply(x_avro,`[[`,"schema"),recursive=F))
    xavro <- lapply(x_avro,`[[`,"data")
    
    if (length(value_types)==1){
      value_types <- value_types[[1]]
      # strip out the type names and 
      # unlist one level
      xavro <- lapply(xavro,function(xi){
        xi <- unlist(xi,recursive=F)
        names(xi)=NULL
        xi
      })
    }else if (length(value_types)==0){
      value_types <- "null"
      warning("No values for Avro \"array\" type: ",name," using \"null\"")
    }
    
    if (xtype=="map"){
      schema$values = value_types
    }else{
      schema$items = value_types
    }
    if (is.field){
      schema$name=NULL
      schema <- list(name=name,type=schema)
    }
  }else {
    stop("Unsupported Avro type: ",xtype," for R type: ",class(x))
  }
  list(schema=schema,data=xavro)
}

make_avro_name <- function(x){
  gsub(".","_",x,fixed=T)
}

## Avro Data Output
## 
## Export R Value to Avro Data File
## 
## Optional \code{name} and \code{namespace} arguments can be used to specify the top-level Avro 
## element \code{name} and \code{namespace}.
## 
## If \code{unflatten=TRUE}, any columns with "." in the name will be combined into nested
## Avro "record" schemas.  For example, the \code{\link{iris}} dataset would be stored as a 
## top-level record containing "Sepal" and "Petal" \code{record} fields and a "Species"
## \code{string} field.  The "Sepal" and "Petal" \code{record} types would each contain 
## "Length" and "Width" \code{double} fields.
##
## If \code{unflatten=FALSE}, any appearances of "." in the column names will be replaced with
## "_" in order to create valid Avro Names.
##
## @param x a data.frame value
## @param file an avro file path
## @param name a character value indicating the Avro "name" for the top-level schema
## @param namespace a character value indicating the Avro "namespace" for the top-level schema
## @param row.names whether or not the "row.names" attribute should be exported with the dataset
## @param codec a character value indicating the Avro codec to use
## @param unflatten whether or not columns with "." in the their names should be "unflattened" 
## 
## @inheritParams avro_make_data
## 
## @export
write.avro <- function(x,file,name="x",namespace=NULL,
                       row.names=T,
                       codec=c("null","deflate","snappy"),
                       unflatten=T){
  if (!is.data.frame(x)){
    if (is.matrix(x)){
      stop("Unsupported type: matrix")
    }
  }else {
    if (unflatten){
      x <- unflatten(x)
    }else {
      names(x) <- make_avro_name(names(x))
    }
  }
  codec <- match.arg(codec,c("null","deflate","snappy"))
  # for convenience grab the name if x was specified by name
  #name <- if (missing(name)){
  #  gsub(".","_",make.names(as.character(deparse(substitute(x)))),fixed=T)
  #}else {
  #  name
  #}
  name <- make_avro_name(name)
  
  
  
  # get the schema
  x_avro_data <- avro_make_data(x,name=name,namespace=namespace,
                                row.names=row.names,
                                unflatten=unflatten)
  x_schema <- x_avro_data$schema
  
  ## If the data is just a generic list, it
  x_schema$name <- name
  if (!is.null(namespace)){
    x_schema$namespace <- namespace
  }
  
  # Convert to JSON and write the temp files
  x_json <- sapply(x_avro_data$data,toJSON)
  json_file <- tempfile(pattern=basename(file),
                        fileext=".json")
  writeLines(x_json,json_file)
  schema_file <- tempfile(pattern=basename(file),
                          fileext=".avsc")
  writeLines(toJSON(x_schema),schema_file)
  
  # Call avro tools
  err <- {
  	if(.Platform$OS.type == "windows") 
  		system2("java",c("-jar",AVRO_TOOLS,
  										 "fromjson",
  										 "--codec",codec,
  										 "--schema-file",sanitize_path(schema_file),
  										 sanitize_path(json_file)),
  						stdout=file,stderr=T)
  	else
  		system(paste("java",
  								 "-jar",AVRO_TOOLS,
  								 "fromjson",
  								 "--codec",codec,
  								 "--schema-file",sanitize_path(schema_file),
  								 sanitize_path(json_file),
  								 ">", file), 
  					 intern = T)}  				 
  if (length(err)>0){
    message(err)
    stop("Error creating Avro file")
  }
  invisible(TRUE)
}