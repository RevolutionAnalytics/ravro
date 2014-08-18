// Copyright 2014 Revolution Analytics
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.#!/bin/bash

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

