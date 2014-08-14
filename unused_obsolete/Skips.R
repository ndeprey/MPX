## Time of first sponsorship
m <- dbDriver("MySQL")
# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)

Skips <- function(start.date, end.date, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, ratings_media_id, ratings_rating, ratings_timestamp, ratings_origin, ratings_elapsed FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND ratings_rating = 'SKIP'",
                        "AND ratings_origin NOT IN ('INTRO','STID')",
                        "AND ratings_timestamp >= '", start.date, "' ",
                        "AND ratings_timestamp <= '", end.date, "' ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  assign("skips",df, envir = .GlobalEnv)
}

  