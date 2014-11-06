r <- read.csv("r_09_15_16.csv")


# Cut_Users <- function(test_cohorts)
uid <- unique(r$ratings_user_id)
TL <- numeric()
P <- numeric()
Q <- numeric()
ratings <- numeric()
other_cohorts <- numeric()

# monday <- data.frame(uid=uid, TLS=TLS, T=T, U=U, other=other_cohorts, ratings=ratings)

# tids <- unique(t$ratings_user_id)
TL <- sapply(uid, function(i) sum(r$ratings_elapsed[r$ratings_user_id==i]))
print("done")
P <- sapply(uid, function(i) length(r$ratings_cohort[r$ratings_user_id==i & r$ratings_cohort=='P']))
print("done")
Q <- sapply(uid, function(i) length(r$ratings_cohort[r$ratings_user_id==i & r$ratings_cohort=='Q']))
print("done")
other_cohorts <- sapply(uid, function(i) length(r$ratings_cohort[!(r$ratings_cohort %in% c('P','Q')) & r$ratings_user_id==i])) 
print("done")
iphone <- sapply(uid, function(i) length(r$ratings_platform[r$ratings_user_id==i & r$ratings_platform=='IPHONE']))
print("done")
android <- sapply(uid, function(i) length(r$ratings_platform[r$ratings_user_id==i & r$ratings_platform=='ANDROID']))
print("done")
ratings <- sapply(uid, function(i) length(r$ratings_rating[r$ratings_user_id==i]))
print("done")

for(i in 1:nrow(monday)){
if(monday$T[i] == monday$ratings[i]){
  monday$cohort[i] <- "T"
}
else if(monday$U[i] == monday$ratings[i]){
  monday$cohort[i] <- "U"
}
else{monday$cohort[i] <- 'other'}
}

median(monday$TLS[monday$T == monday$ratings])
median(monday$TLS[monday$U == monday$ratings])
