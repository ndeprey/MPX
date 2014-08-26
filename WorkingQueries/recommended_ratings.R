default.platforms <- c('IPHONE', 'ANDROID')
library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)
m <- dbDriver("MySQL")
try(setwd("/home/developer/MPX/cronjobs/results"))

Recommended_Channel <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT * 
                        FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND ratings_channel = 'recommended'",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  df$story_num[1] <- 1
  df$session_id[1] <- 1
  pb <- txtProgressBar(max=nrow(df), style=2)
  for(i in 2:nrow(df)){
    setTxtProgressBar(pb, i)
    if (
      ## if any of the criteria below are met, its a new session
      any(
        df$ratings_origin[i]=="STID"&& 
          difftime(df$ratings_timestamp[i],df$ratings_timestamp[i-1],units="min") >100 &&
          grepl("^id",df$ratings_media_id[i],perl=TRUE),
        df$ratings_user_id[i]!=df$ratings_user_id[i-1]
      )) {
      ## start the story count over and augment the session id
      df$story_num[i] <- 1
      df$session_id[i] <- df$session_id[i-1] + 1
    }
    else {
      ## if it's not the start of a new session, augment the story count and and assign the same session id
      df$story_num[i] <- df$story_num[i-1] + 1
      df$session_id[i] <- df$session_id[i-1]
    }
  }
  close(pb)
  
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
  
  return(df)
}

Ratings_To_Sessions <- function(df) {
  
  find.repeats <- function(x, value, len) {
    #' find.repeats(c(0,1,2,2,1,3), 1, 1)
    #' find.repeats(c(0,1,2,2,1,3), 2, 1)
    #' find.repeats(c(0,1,2,2,1,3), 2, 2)
    #' find.repeats(c(0,1,2,2,1,3), 2, 3)
    if(length(x) < len) return(FALSE)
    x.is.value <- (x == value)
    for(i in 1:(length(x)-len+1)) {
      if(all(x[i:(i+len-1)]==value)) return(TRUE)
    }
    return(FALSE)
  }
  
  pb <- txtProgressBar(max=max(df$session_id), style=2)
  session.id <- numeric()
  ratings_user_id <- numeric()
  length.seconds <- numeric()
  start_time <- c()
  story_count <- numeric()
  
  for(i in 1:max(df$session_id)){
    setTxtProgressBar(pb, i)
    session.id[i] <- i
    ratings_user_id[i] <- df$ratings_user_id[df$session_id==i][1]
    length.seconds[i] <- max(df$session_runtime[df$session_id==i])
    start_time[i] <- as.POSIXct(df$ratings_timestamp[(df$session_id==i) & (df$story_num==1)])
    story_count[i] <- max(df$story_num[df$session_id==i])   
  }
  
  sessionsdf <- data.frame(session.id = session.id, ratings_user_id = ratings_user_id, length.seconds = length.seconds, start_time = start_time, story_count = story_count)
  close(pb)
  
  print("done assigning core session metadata")
  
  ## More manipulation for skips and consecutive skips
  pb <- txtProgressBar(max=nrow(sessionsdf), style=2)
  for(i in 1:max(df$session_id)){
    setTxtProgressBar(pb, i)
    sessionsdf$skips[i] <- length(df$ratings_rating[(df$ratings_rating=="SKIP") & (df$session_id==i)])
    sessionsdf$consec.skips.exist[i] <- find.repeats(df$ratings_rating[df$session_id==i],'SKIP',2)
    
    if(sessionsdf$consec.skips.exist[i] == TRUE){
      rl <- rle(as.character(df$ratings_rating[df$session_id==i]))
      sessionsdf$max.consec.skips[i] <- max(rl$lengths[rl$value=="SKIP"])
      sessionsdf$consec.skip.patterns[i] <- length(rl$lengths[(rl$value=="SKIP") & (rl$lengths>=2)])
    }
    
    else{
      sessionsdf$max.consec.skips[i] <- 0
      sessionsdf$consec.skip.patterns[i] <- 0
    }
  }
  close(pb)
  print("done calculating skip metrics")
  
  ## Completes
  
  for(i in 1:max(df$session_id)){
    setTxtProgressBar(pb, i)
    sessionsdf$completes[i] <- length(df$ratings_rating[(df$ratings_rating=="COMPLETED") & (df$session_id==i)])
    sessionsdf$consec.completes.exist[i] <- find.repeats(df$ratings_rating[df$session_id==i],'COMPLETED',2)
    
    if(sessionsdf$consec.completes.exist[i] == TRUE){
      rl <- rle(as.character(df$ratings_rating[df$session_id==i]))
      sessionsdf$max.consec.completes[i] <- max(rl$lengths[rl$value=="COMPLETED"])
      sessionsdf$consec.completes.patterns[i] <- length(rl$lengths[(rl$value=="COMPLETED") & (rl$lengths>=2)])
    }
    
    else{
      sessionsdf$max.consec.completes[i] <- 0
      sessionsdf$consec.completes.patterns[i] <- 0
    }
  }
  close(pb)
  
  ## Mark interestings / thumbups
  
  for(i in 1:max(df$session_id)){
    setTxtProgressBar(pb, i)
    sessionsdf$thumbups[i] <- length(df$ratings_rating[(df$ratings_rating=="THUMBUP") & (df$session_id==i)])
    sessionsdf$consec.thumbups.exist[i] <- find.repeats(df$ratings_rating[df$session_id==i],'THUMBUP',2)
    
    if(sessionsdf$consec.thumbups.exist[i] == TRUE){
      rl <- rle(as.character(df$ratings_rating[df$session_id==i]))
      sessionsdf$max.consec.thumbups[i] <- max(rl$lengths[rl$value=="THUMBUP"])
      sessionsdf$consec.thumbups.patterns[i] <- length(rl$lengths[(rl$value=="THUMBUP") & (rl$lengths>=2)])
    }
    
    else{
      sessionsdf$max.consec.thumbups[i] <- 0
      sessionsdf$consec.thumbups.patterns[i] <- 0
    }
  }
  close(pb)
  
  sessionsdf$activity <- sessionsdf$skips + sessionsdf$thumbups
  
  return(sessionsdf)
}

start.date <- '2014-08-19'
end.date <- '2014-08-21'

recommended_ratings <- Recommended_Channel(start.date,end.date)
write.csv(recommended_ratings,file=paste("recommended_ratings_",end.date,sep=""))

recommended_sessions <- Ratings_To_Sessions(recommended_ratings)
write.csv(recommended_sessions,file=paste("recommended_sessions_",end.date,sep=""))


