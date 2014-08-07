## plot a density plot of the number of sessions at which an ad occurred, at each second

ggplot(data=ratingsdf[(ratingsdf$ratings_origin=="XAPPAD") & (ratingsdf$ratings_rating=="COMPLETED"),]) + 
  aes(x=session_runtime, color=ratings_cohort) + 
  geom_density() + 
  xlim(0,2500)
                                                                                                                                                      

densMode <- function(x){
  td <- density(x)
  maxDens <- which.max(td$y)
  list(x=td$x[maxDens], y=td$y[maxDens])
}