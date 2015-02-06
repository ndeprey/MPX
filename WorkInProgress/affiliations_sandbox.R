library(ggplot2)

ua <- read.csv("/Users/ndeprey/Documents/affiliation_ratings_12_24.csv")

ua$uar_ts <- as.Date(strptime(ua$uar_ts, "%Y-%m-%d"))

ua <- ua[ua$uar_ts > "2014-07-28",]
ua <- ua[ua$uar_affiliation_id > 10,]

qplot(ua$uar_rating)
ggplot(ua, aes(x=uar_rating)) + geom_histogram(binwidth = 0.05, colour = "black", fill="white")

ggplot(ua, aes(x=uar_rating)) + geom_density()

shows <- unique(ua$uar_affiliation_id)
shownames <- unique(ua$thing_title)

write.csv(shows,"/Users/ndeprey/Documents/show_ids.csv")

