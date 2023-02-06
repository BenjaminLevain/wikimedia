## What additional things would you want to operate this application in a production setting?
 -  I would have structured my code into modules and functions to increase maintainability and readability.
 -  I could have also added trackers on errors to debug it more easily and also some loggers to follow that the application is running normally. (I did it with print during my development of the script)
 -  Add a unit test repo with a unit test to test my functions
## What might change about your solution if this application needed to run automatically for each hour of the day?
 -  First, of course, I would need to add a scheduler tool to run the job automatically every hour.
 -  Then, like already said above, I would really need to implement good trackers, loggers,   error exceptions and other alerting systems to follow that the job is correctly executed
-  Optimize the code for scaling running. So be sure that the execution doesn’t take to much time 
