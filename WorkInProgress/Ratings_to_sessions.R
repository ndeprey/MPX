## Start with output of Session_Number
## df <- Session_Number(start.date,end.date)

Ratings_to_session <- function(df) {

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
  start_time <- numeric()
  story_count <- numeric()
  
  for(i in 1:max(df$session_id)){
    setTxtProgressBar(pb, i)
    session.id[i] <- i
    ratings_user_id[i] <- df$ratings_user_id[df$session_id==i][1]
    length.seconds[i] <- max(df$session_runtime[df$session_id==i])
    start_time[i] <- df$ratings_timestamp[(df$session_id==i) & (df$story_num==1)]
    story_count[i] <- max(df$story_num[df$session_id==i])
                             
  }
  
  sessionsdf <- data.frame(session.id = session.id, ratings_user_id = ratings_user_id, length.seconds = length.seconds, start_time = start_time, story_count = story_count)
  close(pb)
  
  print("done assignign core session metadata")
  
  ## More manipulation
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
  
  for(i in 1:max(df$session_id)){
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

  return(sessionsdf)
  
}


