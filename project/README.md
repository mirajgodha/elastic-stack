# Task objective
1) Simulate IOT sensor data
2) Mysql is having the metadata infromation about different sensors there on the n/w
3) Use Logstash to continaously read the IOT sensor data
4) Use logstash to enrich the sensor data, based on meta data stored in Mysql
5) Finally insert the enrichted sensor data to Elasticsearch
6) Creat index pattern on Kibana for the Sensor data stored via logstash
7) Creat the following Kibana charts:
Team 1 - Average humidity per hour per bulding
Team 2 - Average Temperature per hour per bulding
Team 3 - Min/Max humidity per per department
Team 4 - Plot avg temperature on Map
Team 5 - Plot number of readings from each sensor

# Notes
Although have given sample solutions and how to execute them
But left few things, as need team to think throught the things and understand the concepts
You might have to change few things in the solutions to make them work
Install ELK and Mysql
Ensure you are using correct Mysql jar based on Mysql version


# Building a Sensor Data Analytics Application

Please follow the instructions below along with the contents of the chapter to prepare the Elasticsearch index 

1. Create the following index template by executing the command in the your Kibana - Dev Tools. 

```shell
POST _template/sensor_data_template
{
  "index_patterns": [
    "sensor_data*"
  ],
  "settings": {
    "number_of_replicas": "1",
    "number_of_shards": "5"
  },
  "mappings": {
    "properties": {
      "sensorId": {
        "type": "integer"
      },
      "sensorType": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "customer": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "department": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "buildingName": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "room": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "floor": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "locationOnFloor": {
        "type": "keyword",
        "fields": {
          "analyzed": {
            "type": "text"
          }
        }
      },
      "location": {
        "type": "geo_point"
      },
      "time": {
        "type": "date"
      },
      "reading": {
        "type": "double"
      }
    }
  }
}
```

2. If not already done, download the latest 7.x version of logstash for your operating system from https://www.elastic.co/downloads/logstash. At the time of writing this book logstash-7.0.1 was the latest version. Extract logstash on your system.
3. Copy the <b>files</b> directory in your logstash home folder. If logstash was installed at /usr/share/logstash, after copying the files, the structure of files should look like -

```shell
/usr/share/logstash/files/logstash_sensor_data_http.conf
```
4. Ensure that you have either MySQL 5.x or other relational database like postgresql installed on your machine. Please download the appropriate JDBC driver for the database that you are using. At the time of this writing, the latest MySQL JDBC driver (also known as MySQL Connector/J) is 5.1.45. It is downloadable from https://dev.mysql.com/downloads/connector/j/ and works with MySQL 5.5, 5.6, 5.7 versions of MySQL servers.
5. Update the logstash_sensor_data_http.conf file and ensure that **jdbc_driver_library** has correct path to the MySQL JDBC driver jar. Verify that the variable **jdbc_connection_string** has the path to your MySQL database. Create a database called **sensor_metadata** in your MySQL database. Update the **jdbc_password** to the password of your MySQL database's password.
6. Login to the MySQL database using a client such as MySQL workbench, command-line mysql client where SQL scripts can be executed. Execute the script in the files/create_sensor_metadata.sql to load the metadata into the MySQL database.

7. Start logstash from command line, using the following commands

```shell
cd /usr/share/logstash
bin/logstash -f files/logstash_sensor_data_http.conf
```

8. Load the data by running the script data/load_sensor_data.sh from Terminal (if you are using Mac OS or Linux) and Git BASH or other program which supports curl and other basic linux commands if you are using windows.

```shell
./load_sensor_data.sh
```