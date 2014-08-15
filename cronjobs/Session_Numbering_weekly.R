

# default.platforms <- c('IPHONE')
default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
## library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
print("done loading packages")

start.date <- Sys.Date() - 7
end.date <- Sys.Date() - 1

Session_Number <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_user_id, 
                        ratings_media_id, 
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
  
  ## Based on criteria for identifying the beginning of a session, assign a "1" to the start of sessions
  ## and increment the number by one until a new session begins
  df$story_num[1] <- 1
  pb <- txtProgressBar(max=nrow(df), style=2)
  for(i in 2:nrow(df)){
    setTxtProgressBar(pb, i)
    if (
      any(
        df$ratings_origin[i]=="STID"&& 
         difftime(df$ratings_timestamp[i],df$ratings_timestamp[i-1],units="min") >30 &&
          grepl("^id",df$ratings_media_id[i],perl=TRUE),
        df$ratings_user_id[i]!=df$ratings_user_id[i-1]
      )) {
      df$story_num[i] <- 1
    }
    else {
      df$story_num[i] <- df$story_num[i-1] + 1
    }
  }
  close(pb)
  print("completed story numbering, moving onto runtime calculation")
  print(paste("this query includes",
              length(unique(df$ratings_user_id)), 
              "unique users and", 
              length(df$ratings_origin[df$story_num==1]),
              "sessions"))
  
  ## Calculate the running sum of seconds elapsed within the session using the ratings_elapsed field
  pb <- txtProgressBar(max=nrow(df), style=2)
  for(i in 1:nrow(df)){
    setTxtProgressBar(pb, i)
    if(df$story_num[i] == 1){
      df$session_runtime[i] <- df$ratings_elapsed[i]
    }
    else{
      df$session_runtime[i] <- df$session_runtime[i-1] + df$ratings_elapsed[i]
    }
  }
  close(pb)
  
  assign("ratingsdf",df, envir = .GlobalEnv)
  
}

Session_Number(start.date=start.date,end.date=end.date)

write.csv(ratingsdf, file=paste("/home/developer/cron_results/sessions_weekly_",Sys.Date(),".csv",sep=''))
print("done")
### END ###

  
  