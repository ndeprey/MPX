library(lubridate)
library(RMySQL)
library(plyr)
library(reshape2)

m <- dbDriver("MySQL")
default.platforms <- c('IPHONE')
robo.ids <- c(11987982,
              13174734, 1186881532, 1087617437, 1138038188, 9906791, 7456245, 8943206, 
              12526723, 2019184, 100042, 11727645, 10853163, 11657281, 11420786)

### Count Sessions ###
  # A function to mesh ratings data into a table of sessions, each with a unique session id, user id, duration, ratings completed, and start and end times
CountSessions <- function(start.date, end.date, session_timeout=30, platforms=default.platforms, driver=m, group="stage4") {
  # start.date <- '2014-07-12'
  # end.date <- '2014-07-14'
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(driver, group = group)
  SQLstatement <- paste("SELECT * FROM infinite.user_ratings ", 
                        "WHERE ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                        "AND ratings_user_id NOT IN (", paste(robo.ids, collapse=", "), ")", 
                        "ORDER BY ratings_user_id ASC, TIMESTAMP(ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  
  #For every rating, calculate the difference in time (minutes) since the previous rating and add as a column to the dataframe
  for (i in 2:length(df$ratings_rating)) {
    df$ratings_diff[1] <- 0
    df$ratings_diff[i] <- as.numeric(difftime(df$ratings_timestamp[i], df$ratings_timestamp[i-1],units="min"))
  }
  
  # Assign a session ID based on the user ID and a comparison of the ratings time differences vs the session timeout parameter.
  # Determine whether each rating is the start, middle, or end of the session and assign this value 
  df$session_id[1] <- 1
  df$session_pos[1] <- "start"
  for(i in 2:nrow(df)) {
    if(df$ratings_user_id[i] == df$ratings_user_id[i-1] && df$ratings_diff[i] < session_timeout) {
      df$session_id[i] <- df$session_id[i-1]
      df$session_pos[i] <- "middle"
    }
    
    else {
      df$session_id[i] <- df$session_id[i-1] + 1
      df$session_pos[i] <- "start"
      df$session_pos[i-1] <- "end"
    }
  }
  
  # This function is similar to pivot tables in excel, 
  # in which we are grouping the data by session ID and saving as a new data frame
  castdf <- dcast(df,session_id + ratings_user_id ~ ratings_rating,length)
  
  # With this new dataframe compute the total ratings for each session, grab the start and end times from the other df,
  # and compute the duration
  pb <- txtProgressBar(max=nrow(castdf), style=2)
  for(i in 1:nrow(castdf)){
    setTxtProgressBar(pb, i)
    castdf$total_ratings[i] <- sum(castdf[i,c("COMPLETED","DEFER","PASS","SHARE","SKIP","SRCHSTART","START","THUMBUP","TIMEOUT")])
    castdf$session_start[i] <- min(df$ratings_timestamp[df$session_id == i])
    castdf$session_end[i] <- max(df$ratings_timestamp[df$session_id == i])
    castdf$session_duration[i] <- as.numeric(difftime(castdf$session_end[i], castdf$session_start[i], units="min"))
  }
  close(pb)
  assign("Sessions",castdf,envir = .GlobalEnv)
  print(paste("the number of sessions between", start.date, "and", end.date, "was",nrow(Sessions), "across", length(unique(castdf$ratings_user_id)),"unique users"))
  print(paste("total ratings for this period =",nrow(df)))
}

# CountSessions('2014-07-11','2014-07-14',session_timeout=30)

### END ###

###########################################################################
###########################################################################



