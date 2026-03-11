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

Dashboard was developed using Tableau Public  
Since Tableau in the free version doesn't allow direct database connection, .csv files were extracted  
Additionally, polygons of London's boroughs were used to create appropriate map  

To create .csv files commands from temp.txt could be used  
Boroughs GeoJSON link: https://gis-tfl.opendata.arcgis.com/datasets/london-boroughs-1/about  
Tableau dashboard: 
https://public.tableau.com/shared/5WQB8Z7S6?:display_count=n&:origin=viz_share_link

