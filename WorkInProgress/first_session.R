

r <- read.csv("/Users/ndeprey/Documents/first_session_sample.csv")

r$ratings_timestamp <- as.POSIXct(r$ratings_timestamp)
r$create_date <- as.Date(r$ratings_timestamp)
df <- r[r$create_date > as.Date("2014-07-28"),]

### Number Stories in Session ###
pb <- txtProgressBar(max=nrow(df), style=2)

df$story_num <- 1

for(i in 2:nrow(df)){
  setTxtProgressBar(pb, i)
  if(df$ratings_user_id[i] == df$ratings_user_id[i-1]){
    df$story_num[i] <- df$story_num[i-1] + 1
  }
}

pivot_agg <- dcast(df,ratings_origin ~ ratings_rating, fun.aggregate = length)
#pivot_agg <- rbind(pivot_agg, c("Total", colSums(pivot_agg[,-1])))

for( i in 1:nrow(pivot_agg)){
  pivot_agg$instant_skips[i] <- length(df$ratings_rating[
    df$ratings_rating == "SKIP" &
      df$ratings_elapsed <= 4 &
      df$ratings_origin == pivot_agg$ratings_origin[i]]) 
}

pivot_agg <- rbind(pivot_agg, c("all", colSums(pivot_agg[,-1])))

#print(pivot_agg)
pivot_agg <- transform(pivot_agg, COMPLETED = as.numeric(COMPLETED),
                       SHARE = as.numeric(SHARE),
                       SKIP = as.numeric(SKIP),
                       START = as.numeric(START),
                       THUMBUP = as.numeric(THUMBUP),
                       instant_skips = as.numeric(instant_skips)
)

#print(str(pivot_agg))

for (i in 1:nrow(pivot_agg)){
  pivot_agg$TOTAL[i] <- sum(pivot_agg$SKIP[i], pivot_agg$COMPLETED[i], pivot_agg$START[i], pivot_agg$THUMBUP[i], pivot_agg$SHARE[i])
  
  pivot_agg$start_rate[i] <- pivot_agg$START[i] / pivot_agg$TOTAL[i]
  pivot_agg$skip_rate[i] <- pivot_agg$SKIP[i] / pivot_agg$TOTAL[i]
  pivot_agg$instant_skip_rate[i] <- pivot_agg$instant_skips[i] / pivot_agg$TOTAL[i]
  pivot_agg$ins_over_skips[i] <- pivot_agg$instant_skips[i] / pivot_agg$SKIP[i]
  pivot_agg$completion_rate[i] <- pivot_agg$COMPLETED[i] / pivot_agg$TOTAL[i]
  pivot_agg$share_rate[i] <- pivot_agg$SHARE[i] / pivot_agg$TOTAL[i]
  pivot_agg$thumbup_rate[i] <- pivot_agg$THUMBUP[i] / pivot_agg$TOTAL[i]
  
}

num_pivot <- dcast(df,story_num ~ ratings_rating, fun.aggregate = length)

for(i in 1:nrow(num_pivot)){
  num_pivot$skip_rate[i] <- num_pivot$SKIP[i] / sum(num_pivot$COMPLETED[i], num_pivot$SHARE[i], num_pivot$SKIP[i], num_pivot$START[i], num_pivot$THUMBUP[i])
}



