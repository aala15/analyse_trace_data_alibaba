rm(list = ls())

library(dplyr)
library(dagitty)
library(ggdag)
library(igraph)
library(textutils)



file <- "//Users/ha/Desktop/Thesis-trace data/2018/batch_task.csv"
batchTask <- read.csv(file, stringsAsFactors = FALSE, header = TRUE)


##for computional limitaion considering just 10000 batches
data <- tail(batchTask,20000)


###data cleaning####

data <- subset(data,status=="Terminated")
data$Duration <- data$end_time - data$start_time

#removing unknown dependencies which happen at the the end of Strigs
data$task_name <- sub("_$", "", data$task_name)

####handling independent edges (with random names)
NindED<-length(data$task_name[substr(data$task_name, 0, 4 )=="task"])


# create_unique_ids <- function(n=NindED, seed_no = 54321, char_len = floor (log10 (abs (NindED))) + 1 ){
#   set.seed(seed_no)
#   pool <- c(0:9)
#   res <- character(n)
#   for(i in seq(n)){
#     this_res <- paste0(sample(pool, char_len, replace = TRUE), collapse = "")
#     while(this_res %in% res){ # if there was a duplicate, redo
#       this_res <- paste0(sample(pool, char_len, replace = TRUE), collapse = "")
#     }
#     res[i] <- this_res
#   }
#   res
# }

indED <- seq(10000,NindED+10000,1)

for (i in 1:nrow(data)) {
  if (substr(data[i,'task_name'], 0, 4 )=="task" ) {
    data[i,'task_name'] <- indED[i]
  }
}


#handling mergeTasks
mergeT <- subset(data,task_name=="MergeTask")
ohneT <- subset(data,task_name!="MergeTask")

#check how many mergeTask jobs are part of another job  
sum(ifelse(mergeT$job_name %in% ohneT$job_name, 1, 0))

#so it MergeTask are normally (in sample dataset) unique for every job -> Deleting them
data <- subset(data,task_name!="MergeTask")


#handling alphabetic characters and preparing data for creating DAGs
data$task_numebr <- gsub("[^0-9._]", "", data$task_name)
data$task_numebr <- gsub('\\_', '-§-', data$task_numebr)


data <- data %>% 
  group_by(job_name) %>% 
  mutate(totalDAG = paste0(task_numebr, collapse = ","),
         startTime = min(start_time),
         endTime = max(end_time)
         )

#creating DAGs per job
DAGs <- data[!duplicated(data[ ,"job_name"]),c("job_name","totalDAG","startTime","endTime")]

#filtering Dags which containing edges
DAGs <- DAGs[grepl('§',DAGs$totalDAG),]

###calculating node numbers and filtering DAGs with larger than 10 nodes###
#for loop to coun edges
DASs$DAGsN <- NULL
for (i in 1:nrow(DAGs)) {
  DAGsN <- unlist(strsplit(DAGs$totalDAG[i], split=",|§"))
  DAGsN <- gsub("-", "", paste(DAGsN, collapse = ","))
  DAGsN <- unlist(strsplit(DAGsN, split=","))
  DAGsN <- length(unique(DAGsN))
  DAGs$DAGsN[i] <- DAGsN 
}

DAGs <- subset(DAGs,DAGsN>=10)



###creating graphs: list of DAGs (name of each DAG is equal to job name)###
DAGsList <- list()
for (i in 1:nrow(DAGs)) {
  mylist <- unlist(strsplit(DAGs$totalDAG[i], split=",|§"))
  
#defining From edges
  tempList <- mylist[grepl('\\-$',mylist)]
  tempB <- gsub("-", "", paste(tempList, collapse = ","))
  tempFrom <- unlist(strsplit(tempB, split=","))
  #removing empty nodes (possibly errors)
  tempFrom <- unlist(lapply(tempFrom, function(x) x[!x %in% ""]))

#defining To edges
  tempList <- mylist[grepl("^-",mylist)]
  temp <- gsub("-", "", paste(tempList, collapse = ","))
  tempTo <- unlist(strsplit(temp, split=","))
  #removign smpties
  tempTo <- unlist(lapply(tempTo, function(x) x[!x %in% ""]))

#defining Edges
  tempEdges <- gsub("-", "", paste(mylist, collapse = ","))
  tempEdges <- unlist(strsplit(tempEdges, split=","))
  tempEdges <- unique(tempEdges)

#DGA generator
  relations <- data.frame(from=tempFrom, to=tempTo)
  DAGsList[[DAGs$job_name[i]]] <- graph_from_data_frame(relations, directed=TRUE, vertices=tempEdges)
 
}

####### Comparing DAGs using is_isomorphic_to ########

#1 consider only data with DAGs

DAGs <- DAGs[order(DAGs$startTime),]
endTime <- max(DAGs$endTime)
listOfrecurrent <- list()

for ( i in 1:nrow(DAGs)) {
#decreasing search sparce according to tasks periodic time windows
  mins <- seq(DAGs$startTime[i],endTime,900) #15 mins
  hours <- seq(DAGs$startTime[i],endTime,3600) #1 hour
  days <- seq(DAGs$startTime[i],endTime,86400) #1 day

  searchTimes <- c(mins,hours,days)
  searchTimesUp <- searchTimes+30
  searchTimesDown <- searchTimes-30
  seconds <- NULL
  for (z in 1:length(searchTimes)) {
    seconds <-  c(seconds,seq(searchTimesDown[z],searchTimesUp[z],1))
  }
  
#consider only DAGs happening in interesting time windows
  searchDAGs <- NULL
  searchDAGs <- subset(DAGs, DAGs$startTime %in% seconds)
#comparing 
  tempRecurrents <- vector()
  for (j in 1:nrow(searchDAGs)) {
    if (is_isomorphic_to(DAGsList[[DAGs$job_name[i]]], DAGsList[[searchDAGs$job_name[j]]])) {
      tempRecurrents <- append(tempRecurrents, searchDAGs$job_name[j])
    }
  }
  
  listOfrecurrent[[DAGs$job_name[i]]] <- tempRecurrents
}

