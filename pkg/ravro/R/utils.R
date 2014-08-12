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

better_system2 <- function(command,args=character(),stdout="",stderr=""){
  if(.Platform$OS.type == "unix") {
    system2(command,args,stdout=stdout,stderr=stderr)
  } else {
    system(paste(command,paste(args,collapse=" "),">",stdout),intern=T)
  }
}