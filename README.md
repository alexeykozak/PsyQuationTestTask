### **Spark Test Task**

Description of the task located in the TestTaskSpark.pdf 

### **Build**
 
`./gradlew clean shadowJar`

### **Run**
This script creates (or updates) env var SPARK_CONF_DIR. It contains a path to the log4j.properties file.
Also, this script uses spark-defaults.conf in spark-submit command. This config file contains properties required for launching Spark job.
You can define properties like start and end dates, input and output datasets.  

`./launch.sh`