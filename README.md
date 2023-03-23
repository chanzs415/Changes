# Prerequisite
1)Make sure your terminal at the main app folder directory

# Data Source
**Pull data from API**

PS : Use docker images to check Images ID

1) docker run -v C:\Users\USER\Desktop\FypApp\DataStorage\data:/app/data (Python Script Image ID)
2) file should be in FypApp\DataStorage\data

# Data Storage
**Store Data in NameNode**

PS : Use docker ps to check container ID

1) docker cp C:\Users\USER\Desktop\FypApp\DataStorage\data\data_australia.csv (namenode Container ID):/tmp

**Make hdfs directory**

2) docker exec -it namenode /bin/bash

3) hdfs dfs -mkdir -p /data

**Store Data in hdfs**

4) hdfs dfs -put /tmp/data_australia.csv /data/data_australia.csv

**Check if Data is stored in hdfs**

5) hdfs dfs -ls /data

PS: to leave from root of namenode use Ctrl-Z

# Batch Processing

PS : Use docker ps to check container ID

**To go into spark**

1) docker exec -it (spark Container ID) bash

2) inside bash use spark-bash

**Load Data from HDFS**

3) val df = spark.read.csv("hdfs://namenode:9000/data/data_australia.csv")

PS: to leave from Scala cmd use Ctrl-D
PS: to leave from bash cmd use Ctrl-Z


**UPDATED

3 BAT files, startup.bat, startup2.bat, shutdown.bat

1) startup.bat
- Run this code in your terminal
    - (your directory)/startup.bat
* This bat files runs the processes of 
    - Compose file
    - Data ingestion
    - Transferring of files from storage container to hdfs to spark container through volume
    - Processes the data through spark container, outputs a cleaned csv
    - Transfer clean csv to volume
    - Start and run the visualization app container

2) startup2.bat
- Run this code in a separate terminal from startup.bat, as previous terminal is running streamlit
    - (your directory)/startup.bat
* This bat file runs the processes of
    - Transferring output file from volume to visualapp container

3) Open localhost:8501 for streamlit
4) shutdown.bat
- Run this code in the first terminal
    - (your directory)/shutdown.bat
* This bat file runs docker-compose down to clear the containers and cleans up docker.
