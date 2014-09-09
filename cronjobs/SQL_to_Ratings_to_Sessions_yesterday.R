#########################################################
########### SQL_to_Ratings_to_Sessions ##################
#########################################################

#### This function: 
######## starts with a SQL query, 
######## pulls down ratings ordered by user id and timestamp
######## determines the session start and end points
######## groups them into sessions assigning key metadata in a new dataframe
######## performs key analysis on skips and completes
######## and finally writes to a csv in the same directory

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

start.date <- Sys.Date() - 1
end.date <- Sys.Date() - 1
print(paste("for start date of",start.date,"and end date of",end.date))

##
## Declare Functions ##
##

Session_Number <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
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
  
  ## Based on criteria for identifying the beginning of a session, assign a "1" to the start of sessions
  ## and increment the number by one until a new session begins
  df$story_num[1] <- 1
  df$session_id[1] <- 1
  pb <- txtProgressBar(max=nrow(df), style=2)
  for(i in 2:nrow(df)){
    setTxtProgressBar(pb, i)
    if (
      ## if any of the criteria below are met, its a new session
      any(
        df$ratings_origin[i]=="STID"&& 
         difftime(df$ratings_timestamp[i],df$ratings_timestamp[i-1],units="min") >30 &&
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
  print("completed story numbering, moving onto runtime calculation")
  print(paste("this query includes",
              length(unique(df$ratings_user_id)), 
              "unique users and", 
              length(df$ratings_origin[df$story_num==1]),
              "sessions"))
  
  ## mark the last story in each session
  df$is.last[1] = FALSE
  for(i in 2:nrow(df)){
    setTxtProgressBar(pb, i)
    if(df$story_num[i]!=1){
      df$is.last[i] <- FALSE
    }
    else{
      df$is.last[i-1] <- TRUE
    }
  }
  close(pb)
  print("done marking last story")
  
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




## Optional write to csv ###
## write.csv(ratings_last3, file=paste("session_number",enddate,"_3_days.csv",sep=""))



## This function starts with the output of Session_Number and converts it into
## a df of sessions aggregating groups of ratings

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
  session.cohort <- c()
  local.newscast <- c()
  local.story <- c()
  
  for(i in 1:max(df$session_id)){
    setTxtProgressBar(pb, i)
    sdf <- df[df$session_id==i,]
    session.id[i] <- i
    session.cohort[i] <- sdf$ratings_cohort[1]
    ratings_user_id[i] <- sdf$ratings_user_id[1]
    length.seconds[i] <- max(sdf$session_runtime)
    start_time[i] <- as.POSIXct(sdf$ratings_timestamp[1])
    story_count[i] <- max(sdf$story_num)
    local.newscast[i] <- any(sdf$ratings_origin=="LOCALNC")
    local.story[i] <- length(sdf$ratings_origin[sdf$ratings_origin=='ORGZN'])
  }
  
  sessionsdf <- data.frame(session.id = session.id, ratings_user_id = ratings_user_id, session.cohort = session.cohort,
                           length.seconds = length.seconds, start_time = start_time, 
                           story_count = story_count, local.newscast=local.newscast, local.story=local.story)
  close(pb)
  
  print("done assigning core session metadata")
  
  ## Count the skips, find repeat patterns in skips
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
  
  ## count the completes, find the repeat patterns in completes
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
  
  return(sessionsdf)
  
}

#########################
### Run the Functions ###
#########################

ratings_yesterday <- Session_Number(start.date,end.date)

try(write.csv(ratings_yesterday, file=paste("ratings_yesterday_",end.date,".csv",sep='')))

print("Completed all Session Numbering. Moving onto Ratings_to_Sessions function")

Sessions.last.3 <- Ratings_To_Sessions(ratings_yesterday)
### write to csv
write.csv(Sessions.last.3, file=paste("sessions_yesterday_",end.date,".csv",sep=''))

query.end.time <- Sys.time()
query.runtime <- as.numeric(query.end.time - query.start.time) / 60
# seconds.per.row <- (query.runtime*60)/nrow(ratings_yesterday)
print(paste("this query started at",query.start.time,"and finished at",query.end.time))
# print(paste("total runtime was",query.runtime,"minutes"))
# print(paste("processing speed was",seconds.per.row,"seconds per row"))

##### END ######

