#
# Run with:
# source("nn_forecast.R")
#
#   nn_forecast(fname)
#		     t0
#   a40834n 	01/06/2011 21:00 	M 40
#   this set originally is part of mimic2wdb/CINC/train-set/H1/a40834n
#   
#   21 hours before t0:
#   nn_forecast("data/a40834n.csv", 12, "2011-06-01 00:00:00", "2011-06-01 21:00:00")
#   4 hours before t0:
#   nn_forecast("data/a40834n.csv", 3, "2011-06-01 17:00:00", "2011-06-01 21:00:00")
#   9 hours before t0: with ma_order: 3, 5, 6(ok)=NNAR(15,8)
#   nn_forecast("data/a40834n.csv", 6, "2011-06-01 12:00:00", "2011-06-01 21:00:00")
#
#   nn_forecast("data/a40439n.csv", 12, "2008-09-04 08:00:00", "2008-09-04 18:30:00")
#
#   nn_forecast("data/a40493n.csv", 12, "2016-07-20 00:00:00", "2016-07-20 08:00:00")
#   
#   nn_forecast("data/a40493n.csv", 8, "2016-07-19 20:00:00", "2016-07-20 08:00:00")
#
nn_forecast <- function(fname, ma_order, start_time, t0_time){

library(forecast)
library(smooth)

t <- read.csv(fname, fill = FALSE, na.strings = "-", quote="'")
# format times and dates
td <- strptime(t$Timeanddate, "[%H:%M:%S %d/%m/%Y]")
t <- cbind(t,td)

t1 <- t[t$ABPMean!=0.0, c("td","ABPMean")]
t2 <- na.omit(t1)

# smooth a bit the data using a simple moving average
ABPSmooth <- ma(t2$ABPMean, order=ma_order)
ABPSmooth <- as.data.frame(ABPSmooth)
colnames(ABPSmooth) <- c("ABPSmooth")

# put it back
t3 <- cbind(t2,ABPSmooth)
# after smoothing some NA are introduce before and after the ts
t3 <- na.omit(t3)

plot(t3$ABPMean, type='l')
lines(t3$ABPSmooth, col='red')

abp_smooth_t0 <- t3[t3$td > start_time & t3$td < t0_time, c('ABPSmooth')]
abp_smooth <- t3[t3$td > start_time, c('ABPSmooth')]

# fit a NN model
fit <- nnetar(abp_smooth_t0)

#dev.new()
plot(forecast(fit,h=60))
lines(abp_smooth, col='red')
lines(abp_smooth_t0)


}
