# determine the time of last available "non zero" measurement for the DB
# the day of the last "non zero" measurement and today's day is the window to query.
# if determanation fails: retry 3 times, if fails: abort: send alert (mail).  
# if the window is more than 5 days. send alert (mail) and contiunue regardless.

#send query to eon. if fails, retry in 10 minutes 3 times. I fails: abort: send alert (mail)

# check data rudimentarily, and for consistency.
# if fine write data in the datatabase. Id data with the same timestamp exist, overwrite it. 
# log: the total number of meausrements written in the database. 
#   number of kmeasurements overwritten with the same data (identical), 
#   number of measurements overwritten with new data, 
#   new measurement point entered 
# if fails: retry 3 times, if fails: abort: send alert (mail)

#  mark succesfull completion or failure in a log