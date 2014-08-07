## Time of first sponsorship
m <- dbDriver("MySQL")
# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)

Avg_Spons_Time <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, ratings_media_id, ratings_rating, ratings_timestamp, ratings_origin, ratings_elapsed FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND ratings_timestamp >= '", start.date, "' ",
                        "AND ratings_timestamp <= '", end.date, "' ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  

  ## calculate timediffs
  pb <- txtProgressBar(max=nrow(df), style=2)
  df$ratings_timestamp <- as.POSIXct(df$ratings_timestamp)
  for(i in 2:nrow(df)){
    setTxtProgressBar(pb, i)
    df$timediff[i] <- difftime(df$ratings_timestamp[i],df$ratings_timestamp[i-1],units="min")
  }
  
  print("done assigning timediffs")
  close(pb)
  
  ## Find Station ID that proceeded the previous rating by the session timeout value
  ## session_starts <- which(df$ratings_origin=="STID" & grepl("^id",df$ratings_media_id,perl=TRUE) & df$timediff > session_timeout)
  
  ## Find sponsorships and create a dataframe
  spons <- which(df$ratings_origin=="SPONS")
  print("found sponsorhips")
  sponsdf <- data.frame(spons=spons)
  print("set dataframe")
  print(head(sponsdf))
  assign("df",df,envir = .GlobalEnv)
  assign("sponsdf",sponsdf,envir = .GlobalEnv)
  
  ## for each spons, find the corresponding session start, fetch the timestamps, calculate the difference
  pb <- txtProgressBar(max=nrow(sponsdf), style=2)
  print("pb set")
  for(i in 1:nrow(sponsdf)){
    setTxtProgressBar(pb, i)
    sponsdf$start[i] <- which(df$ratings_origin=="STID" & 
                             df$ratings_user_id == df$ratings_user_id[spons[i]] & 
                             grepl("^id",df$ratings_media_id,perl=TRUE) & 
                             df$timediff > 30 & 
                             df$ratings_timestamp < df$ratings_timestamp[spons[i]])
    sponsdf$spons_time[i] <- df$ratings_timestamp[sponsdf$spons[i]]
    sponsdf$start_time[i] <- df$ratings_timestamp[sponsdf$start[i]]
  }
  close(pb)
  print("done calculating diffs")
  print(head(sponsdf))
  
  for(i in 1:nrow(sponsdf)){
    sponsdf$time_of_spons[i] <- as.numeric(difftime(sponsdf$spons_time[i],sponsdf$start_time[i],units="min"))
  }
 
  assign("Session_spons",sponsdf,envir = .GlobalEnv)
}
