# NBA-Live Flask Based Web Application
This application provides  NBA Live game statistics(updated every 10 minutes).NBA Seasons included are from 1979 to current date.This is a prototype of Cloud application deployed on AWS EC2 instance. External API calls are made to https://www.balldontlie.io/api/v1/stats
and https://www.balldontlie.io/api/v1/players in order to obtain the Live scores and the list of all NBA players with their details, which is stored in a cassandra database.

 Following were implemented for the Cloud Computing mini project:
 
 -REST-based services interface.
 -Interaction with external REST services.
 -Use of on an external Cloud database for persisting information.
 -Support for cloud scalability, deployment in a container environment.
 -Cloud security.
