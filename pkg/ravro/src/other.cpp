#include <Rcpp.h> 

using namespace Rcpp;

// [[Rcpp::export("default.if.null")]]

List default_if_null(List x, RObject default_value) {
	List y(x.size());
	for(unsigned int i = 0; i < x.size(); i++) {
		if(as<RObject>(x[i]).isNULL()) {
			y[i] = default_value;}
		else {
			y[i] = x[i];}}
	return y;}
	
// [[Rcpp::export("fill.with.NAs.if.short")]]

List NAs_if_short(List x, unsigned int nc) {
	List y(x.size());
	for(unsigned int i = 0; i < x.size(); i++) {
	  List xi = as<List>(x[i]);
		if(xi.size() != nc) {
		  List filler(nc, NA_LOGICAL);
			y[i] = filler;}
		else {
			y[i] = x[i];}}
	return y;}
	
	