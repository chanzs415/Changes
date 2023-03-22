::start building and running containers
docker-compose -f C:\Users\USER\Fyapp\Changes\FypApp\docker-compose.yml up -d

::Get data from API to store in container's /app/data
::docker exec -it sparkcontainer python3 main.py

::Use container's volume to transfer data between containers
:: - transfer from sparkcontainer to hadoop_namenode volume that was specified in compose file
::docker cp sparkcontainer:/app/ hadoop_namenode/   

:: - transfer from hadoop_namenode volume to /tmp in namenode
::docker cp hadoop_namenode/data namenode:/tmp    

:: -put data from /tmp into hdfs
::docker exec -it namenode hdfs dfs -put /tmp/data/ /data

::Run script that processes the data
::docker exec -it sparkcontainer spark-submit process.py

::Store processed csv in local container

::Copy csv into hadoop_namenode folder to view

::Remove containers
::docker-compose -f C:\Users\USER\Fyapp\Changes\FypApp\docker-compose.yml down
:: val df = spark.read.csv("hdfs://namenode:9000/data/data_australia.csv")


:: To run this file,  C:\Users\USER\Fyapp\Changes\Fypapp\automate.bat
::docker cp C:\Users\USER\Downloads\combined_csv.csv namenode:/tmp


