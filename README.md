This project is created in order to prepare data from https://api-portal.tfl.gov.uk for analysis.
API retrieves data of accidents which happened in London transportation system from 2005 to 2019.
Format of response is JSON document with details of accident (time, location, severity, transport, victims, etc.).
For the purposes of analysis (with the usage of BI tools), relational database is more suitable.   

To run using Docker:
1) Start Docker  
2) docker compose build  
3) docker compose up -d postgres   
4) docker compose run load  
5) docker compose run etl

Dashboard with the analysis is yet to be developed
