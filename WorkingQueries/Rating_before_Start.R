
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
try(setwd("/home/developer/MPX/cronjobs/results"))

Ratings_Before_Start <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT * 
                        FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))

  starts <- df[df$ratings_rating=='START',]
  
  pb <- txtProgressBar(max=nrow(df), style=2)
  for(i in 1:nrow(starts)){
    setTxtProgressBar(pb, i)
    matched <- which(
      df$ratings_user_id==starts$ratings_user_id[i]
      & df$ratings_rating=='START'
      & df$ratings_timestamp == starts$ratings_timestamp[i])
    as.character(starts$prev1[i] <- df$ratings_rating[matched - 1])
    as.character(starts$prev2[i] <- df$ratings_rating[matched - 2])
    as.character(starts$prev3[i] <- df$ratigns_rating[matched - 3])
  }
  close(pb)
  
  
  return(starts)
}


start.date <- '2014-08-19'
end.date <- '2014-08-21'

startsdf <- Ratings_Before_Start(start.date,end.date)
write.csv(startsdf,file=paste("startsdf_",end.date,sep=""))


