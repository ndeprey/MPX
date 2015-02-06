u <- read.csv("/Users/ndeprey/Documents/all_users_1_8_w_emails.csv")
> u$create_date <- as.Date(strptime(u$create_date, "%Y-%m-%d"))
> u$last_listen_date <- as.Date(strptime(u$last_listen_date, "%Y-%m-%d"))
> u$range_active <- 1 + as.numeric(u$last_listen_date - u$create_date)
> u$pct_active <- u$total_active_days / u$range_active

old <- u[u$range_active > 42 & u$last_listen_date > "2014-12-01",]
> light <- old[old$pct_active > 0.05 & old$pct_active < 0.076,]
> medium <- old[old$pct_active > 0.076 & old$pct_active < 0.134,]
> heavy <- old[old$pct_active > 0.134 & old$pct_active < 0.28,]
> survey_users <- rbind(light[sample(nrow(light),2500),],medium[sample(nrow(medium),5000),],heavy[sample(nrow(heavy),2500),])