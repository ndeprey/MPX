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

start.date <- Sys.Date() - 2
end.date <- Sys.Date() - 2
print(paste("for start date of",start.date,"and end date of",end.date))

##
## Declare Functions ##
##

Ratings_to_user_days <- function(start.date, end.date, platforms=default.platforms, driver=m, group="stage4") {
  
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
  df$user_day_id <- paste(as.character(df$ratings_user_id),as.Date(df$ratings_timestamp),sep=" ")
                          
                          
  
  ## Initialize column measurements you'll need
  user_day_ids <- unique(df$user_day_id)
  user_day_id <- c()
  user_id <- c()
  TLM <- numeric()
  skip_rate <- numeric()
  mark_rate <- numeric()
  start.time <- c()
  date <- c()
  
  pb <- txtProgressBar(max=length(unique(df$user_day_id)), style=2)
  
  for(i in 1:length(unique(df$user_day_id))){
    setTxtProgressBar(pb, i)
    sdf <- df[df$user_day_id==user_day_ids[i],]
    l <- nrow(sdf)
    user_day_id[i] <- sdf$user_day_id[1]
    user_id[i] <- sdf$ratings_user_id[1]
    skip_rate[i] <- nrow(sdf[sdf$ratings_rating=="SKIP",]) / l
    mark_rate[i] <- nrow(sdf[sdf$ratings_rating=="THUMBUP",]) / l
    TLM[i] <- sum(sdf$ratings_elapsed) / 60
  }
  
  close(pb)
  user_days <- data.frame(user_day_id = user_day_id, user_id = user_id, skip_rate = skip_rate, mark_rate = mark_rate, TLM = TLM)
  
  print("done")
  
  return(user_days)
}

user_days <- Ratings_to_user_days(start.date=start.date, end.date=end.date)

  