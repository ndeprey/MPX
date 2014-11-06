x <- read.csv("/Users/ndeprey/Documents/xapp_investigation.csv")
x$ratings_timestamp <- as.POSIXct(x$ratings_timestamp)

timediffs <- as.numeric(diff(x$ratings_timestamp))
head(timediffs)

timediffs[length(timediffs) + 1] <- 0
x$diffs <- timediffs

xapps <- x[x$ratings_origin %in% c("XAPPAD","XAPPPROMO"),]
unplayed_xapps <- x[(x$ratings_origin %in% c("XAPPAD","XAPPPROMO")) & (x$diffs == 0) & (x$ratings_rating == 'COMPLETED'),]

table(unplayed_xapps$ratings_origin)
table(xapps$ratings_origin)
=