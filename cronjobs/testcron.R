ddate <- Sys.time()
dddate <- gsub(" ","",ddate)
newdate <- gsub(":","",dddate)

write.csv(iris,file=paste(newdate,".csv",sep=""))

try(setwd("/home/developer/MPX/cronjobs/results"))

write.csv(iris, file=paste("in_newdir_",newdate,".csv",sep=""))


