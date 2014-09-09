### Get sparse matrix ###

Get_ratings_story_info <- function(start.date, end.date, origins, platforms=default.platforms, driver=m, group="stage4") {
  #start.date <- '2014-08-31'
  #end.date <- '2014-08-31'
  # group = "stage4"
  #Fetch the ratings data from input parameters and sort by user id and timestamp  
  con <- dbConnect(m, group = group)
  SQLstatement <- paste("SELECT user_ratings.ratings_user_id, 
                        user_ratings.ratings_story_id,
                        user_ratings.ratings_rating, user_ratings.ratings_timestamp ",
                        "FROM infinite.user_ratings ", 
                        "WHERE user_ratings.ratings_platform IN ('", paste(default.platforms, collapse="', '"), "') ",
                        "AND user_ratings.ratings_origin IN ('", paste(origins, collapse="', '"), "') ",
                        "AND DATE(user_ratings.ratings_timestamp) >= '", start.date, "' ",
                        "AND DATE(user_ratings.ratings_timestamp) <= '", end.date, "' ",
                        "ORDER BY user_ratings.ratings_user_id ASC, TIMESTAMP(user_ratings.ratings_timestamp) ASC", sep='')
  rs <- dbSendQuery(con, SQLstatement)
  df <- fetch(rs, n=-1)
  print(paste("the query has", nrow(df), "ratings"))