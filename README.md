# spark-analytic-jobs
In this project, where you can test your data engineering skills, you will deal with problems that include the
following jobs:
 - Batch Processing
 - Stream Processing
 - DevOps
![image](https://user-images.githubusercontent.com/100845374/182556243-152c300e-e88e-484a-b054-29f84c7261db.png)
The diagram above shows an end-to-end Data Pipeline. Here are Data Producer, Kafka, Flink and Spark
applications.<br>
What was implemented:
   - Batch Spark application(s) with data in file system.
   - Flink application using Kafka topics which is the DataProducer application writes data.
   - Dockerized all applications and provide a single command (script or docker-compose up) which runs
     all applications.
   - In project is using Scala.

**Problem Details**
**Data**<br>
You will work on orders and products data. <br>
Here are their schemas:<br>
![img_2.png](img_2.png)
<br>
