r <- read.csv('/Users/ndeprey/Documents/podcasts_listens_12_8.csv')
affils <- read.csv('/Users/ndeprey/Documents/user_affiliations_12_8.csv')

str(r)
r$create_date <- as.Date(strptime(r$create_date, "%Y-%m-%d"))
r$last_listen_date <- as.Date(strptime(r$last_listen_date, "%Y-%m-%d"))
r <- r[r$last_listen_date > "2014-07-27",]
