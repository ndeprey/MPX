## Declare key variables and load libraries

library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
default.platforms <- c('IPHONE', 'ANDROID')

## IF YOU GET AN ERROR for too many connections open, use the following code
cons <- dbListConnections(MySQL())
for (con in cons){
  dbDisconnect(con) }

#For measuring speed of query
query.start.time <- Sys.time()

### this only works if you're on the dev server
try(setwd("/home/developer/MPX/cronjobs/results"))

start.date <- Sys.Date() - 14
end.date <- Sys.Date() - 1
print(paste("for start date of",start.date,"and end date of",end.date))

##
## Declare Functions ##
##

Get_user_matrix <- function(start.date, end.date, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        ratings_media_id, 
                        ratings_story_id,
                        ratings_rating, ratings_timestamp, 
                        ratings_origin, 
                        ratings_elapsed, 
                        ratings_cohort 
                        FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  ## Convert timestamp strings to POSIXct
  df$ratings_timestamp <- as.POSIXct(df$ratings_timestamp)
  
  uids <- unique(df$ratings_user_id)
  TLM <- numeric()
  Days.active <- numeric()
  HRNCs <- numeric()
  searches <- numeric()
  skips <- numeric()
  instant_skips <- numeric()
  marks <- numeric()
  ratings <- numeric()
  
  pb <- txtProgressBar(max=length(uids), style=2)
  for(i in 1:length(uids)){
    setTxtProgressBar(pb, i)
    tdf <- df[df$ratings_user_id==uids[i],]
    TLM[i] <- sum(tdf$ratings_elapsed) / 60
    Days.active[i] <- length(unique(as.Date(tdf$ratings_timestamp)))
    HRNCs[i] <- nrow(tdf[tdf$ratings_origin=="HRNC",])
    searches[i] <- nrow(tdf[tdf$ratings_rating %in% c("SRCHSTART","SRCHCOMPL"),])
    skips[i] <- nrow(tdf[tdf$ratings_rating == "SKIP",])
    marks[i] <- nrow(tdf[tdf$ratings_rating == "THUMBUP",])
    ratings[i] <- nrow(tdf)                 
  }
  
  close(pb)
  
  users <- data.frame(uid = uids, TLM = TLM, days.active=Days.active, HRNCs = HRNCs, searches = searches, skips = skips, marks = marks, ratings = ratings)
  
  return(users)
}

users <- Get_user_matrix(start.date, end.date)
write.csv(users, file=paste("users",start.date,end.date,sep="_"))
