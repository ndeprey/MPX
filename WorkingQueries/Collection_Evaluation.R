
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

Collection_evaluation <- function(start.date, end.date, origin, platforms=default.platforms, driver=m, group="stage4") {
  
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT ratings_story_id, 
                          ratings_rating, 
                          ratings_user_id, 
                          ratings_cohort, 
                          ratings_timestamp,
                          thing_title as showname, 
                          thing_id as seamus_agg, 
                          thing_type_id,
                          object_child_type_id from infinite.user_ratings
                          JOIN
                          cms.object_assign on user_ratings.ratings_story_id = object_assign.object_child_id AND object_assign.object_rel_type_id = 20
                          JOIN
                          cms.thing on object_assign.object_parent_id = thing.thing_id AND thing.thing_type_id = 21
                          WHERE ratings_origin = '", origin, "' ",
                          "AND ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                          "AND DATE(ratings_timestamp) >= '", start.date, "' ",
                          "AND DATE(ratings_timestamp) <= '", end.date, "' ",
                          "AND ratings_user_id < 1000000000 
                          limit 1000000000000", sep=("")
  )

  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))
  
  shows <- dcast(df, showname + seamus_agg ~ ratings_rating, fun.aggregate = length)
  shows$TOTAL <- rowSums(shows[,-c(1,2)])
  shows <- shows[order(-shows$TOTAL),]
  
  return(shows)
}


prompt_args <- function(){
  start.date <- readline("What is the start date?  e.g. '2015-01-26' ")
  end.date <- readline("What is the (inclusive) end date? e.g. '2015-01-28' ")
  origin <- readline("What is the origin name? e.g. 'RATED' ")
  
  args <- list(start.date = start.date,
               end.date = end.date,
               origin = origin)
  
  return(args)
}
  
args <- prompt_args()

shows_df <- Collection_evaluation(start.date = args$start.date,
                                  end.date = args$end.date,
                                  origin = args$origin)


write.csv(shows,"/Users/developer/podcast_reports")

print(shows_df)
