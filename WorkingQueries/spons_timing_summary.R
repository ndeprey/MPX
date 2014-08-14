##########################################################
## Sample Queries for analyzing session-level stuff ######
##########################################################


## Figure out the median time at which a user heard a piece by origin, in this case the DONATE
summary(ratingsdf$session_runtime[ratingsdf$ratings_origin=='DONATE'&ratingsdf$story_num %in% c(11,12,13)])

## plot a density plot of the number of sessions at which an ad occurred, at each second
ggplot(data=ratingsdf[(ratingsdf$ratings_origin=="SPONS") & (ratingsdf$ratings_rating=="COMPLETED"),]) + 
  aes(x=session_runtime, color=ratings_cohort) + 
  geom_density() + 
  xlim(0,2500)
                                                                                                                                                      
### Incomplete but intended to find the peak of the ggplot graphs
densMode <- function(x){
  td <- density(x)
  maxDens <- which.max(td$y)
  list(x=td$x[maxDens], y=td$y[maxDens])
}