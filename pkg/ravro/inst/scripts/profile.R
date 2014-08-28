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
# limitations under the License.#!/bin/bashlibrary(ravro)

# Choose a target data object
df=read.avro(system.file("data/yield1k.avro",package="ravro"))
# Choose a total amount of time in seconds to spend profiling
max_time <- as.difftime(100,units="secs")
total_time <- 0
start_time <- Sys.time()

runtime_vec <- numeric(0)
n_vec <- numeric(0)

n_current <- nrow(df)
while(total_time < max_time){
  df <- rbind(df,df)
  df_avro <- tempfile(fileext=".avro")
  ravro:::write.avro(df,file=df_avro)
  new_time <- system.time(read.avro(df_avro))
  runtime_vec <- c(runtime_vec,new_time["elapsed"])
  n_vec <- c(n_vec,nrow(df))
  total_time <- Sys.time()-start_time
}

if (require(ggplot2)){
  ggplot(data.frame(time=runtime_vec,n=n_vec),aes(x=n,y=time)) + geom_line()
  ggsave("ravro_runtime_performance.png")
}else {
  png("ravro_runtime_performance.png")
  plot(n_vec,runtime_vec,type="l")
  dev.off()
}
