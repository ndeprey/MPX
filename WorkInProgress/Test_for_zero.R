A.medians <- numeric()
B.medians <- numeric()
diffs <- numeric()
means <- numeric()

for( i in 1:10000){
  A <- r[sample(nrow(r), 5000) ,]
  B <- r[sample(nrow(r), 5000) ,]
  diffs[i] <- median(A$TotalSeconds) - median(B$TotalSeconds)
}

for(i in 1:length(diffs)){
  means[i] <- mean(diffs[1:i])
}
