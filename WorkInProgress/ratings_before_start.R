r$ratings_timestamp <- as.POSIXct(r$ratings_timestamp)
pb <- txtProgressBar(max=nrow(r), style=2)
for(i in 1:nrow(r)-1){
  setTxtProgressBar(pb, i)
  r$dftime[i] <- as.numeric(difftime(r$ratings_timestamp[i],r$ratings_timestamp[i+1],units="secs"))
}

prev1 <- c()
prev2 <- c()
prev3 <- c()
orig <- c()

pb <- txtProgressBar(max=nrow(r), style=2)
for(i in 4:nrow(r) - 1){
  setTxtProgressBar(pb, i)
  if((r$ratings_rating[i] == 'START') &
       (r$ratings_user_id[i] == r$ratings_user_id[i-1]) &
       (r$dftime[i] > 3600)){
    prev1 <- c(prev1, as.character(r$ratings_rating[i-1]))
    prev2 <- c(prev2, as.character(r$ratings_rating[i-2]))
    prev3 <- c(prev3, as.character(r$ratings_rating[i-3]))
    orig <- c(orig, r$ratings_origin[i])
  }
}

starts <- data.frame(origin=orig,
                     prev1=prev1,
                     prev2=prev2,
                     prev3,prev3)
