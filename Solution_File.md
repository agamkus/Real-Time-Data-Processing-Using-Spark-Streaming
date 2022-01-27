# Real-Time-Data-Processing-Using-Spark-Streaming

## Objective
A stream of data is nothing but a continuous inflow of data, for example, a live video or Amazon’s log data. There is no discrete start or end to this stream as it keeps flowing forever. Since this stream is continuous, it is also usually high-volume data, which means the size of the incoming data would be large. Data streams are usually processed in near real-time.

Streams are used for the following important reasons:
1.	They help capture real-time reactions, which can be used for various analytical purposes.
2.	Since it is near-real-time processing, they can be used for fraud detection or for initiating prompt responses as per the needs of the situation.
3.	They can be used for scaling up or scaling down hardware as per incoming load.

We have to process the data in micro batches which is a near real time processing. Here, our objective is to create a Spark Streaming application and perform below operations which are handled in near real-time:
- Creation of a Basic Spark Streaming Application
- Different Output Modes
- Triggers
- Transformation
- Joins with Streams
- Window Functions

## Prerequisites
- AWS EC2 instance
- NetCat Service
- Basict understanding of AWS

## Code Flow
Below is the general code flow of the Spark Streaming application:
1.	### Create a SparkSession:
    Start writing the code by building a **SparkSession** using the **getOrCreate()** function:
    
    ````
    spark = SparkSession \
      .builder \
      .appName("StructuredSocketRead") \
      .getOrCreate()
      ````
2.	### Read from source:
	  Tell Spark that we would be reading from a socket. Below is the code:
    ````
    Lines = spark.readStream \
      .format("socket") \
      .option("host","localhost") \
      .option("port",12345).load()
    ````
    
3.	### Start:
    Next, we’ll use the writeStream() method and specify the output mode. We’ll also call the start() action at the last. Remember, we need to tell Spark where we want to write our stream to. In our case it is the console. Below is the code line:

    ````
    query = lines.writeStream \
      .outputMode("append") \
      .format("console") \
      .start()
    ````

4.	### AwaitTermination:
    At last, we’ll run below code to tell Spark to run continuously until some external termination is called:

    ````
    query.awaitTermination()
    ````

## Segment 1: A Spark Streaming Application
Let’s first create an application using Spark Streaming which reads data from socket and prints output on console. Below are the steps:

1. Login to Amazon AWS console and launch EC2 instance.
2. Switch to the root user using **sudo -i** command.
3. Run below commands as root user to install Netcat Service. It is required to open sockets.
    ````
    yum update -y
    yum install -y nc
    ````

4.	Exit from the root user. Let’s create a directory **coding_labs** and make it current directory using below commands:

    ````
    mkdir coding_labs
    cd mkdir_labs
    ````
    ![image](https://user-images.githubusercontent.com/56078504/151303373-04b08fbc-b6ec-4cb5-b338-ab3ecef49594.png)

5. Run the below command to create a file **read_from_socket.py**:

    ````
    vi read_from_socket.py
    ````
    ![image](https://user-images.githubusercontent.com/56078504/151303589-b2a41cc6-ac4e-4270-9fef-2deb730c93be.png)

6. Below is the complete code. Hit the ‘**I**’ key to turn the console into edit mode and paste the below codes to the file. Press ‘**Esc**’ key and enter **:wq!** to save the file.

    ````
    from pyspark.sql import SparkSession

    spark = SparkSession  \
      .builder  \
      .appName("StructuredSocketRead")  \
      .getOrCreate()

    lines = spark  \
      .readStream  \
      .format("socket")  \
      .option("host","localhost")  \
      .option("port",12345)  \
      .load()

    query = lines  \
      .writeStream  \
      .outputMode("append")  \
      .format("console")  \
      .start()

    query.awaitTermination()
    ````
    ![image](https://user-images.githubusercontent.com/56078504/151303743-fcf6ad1b-c9a4-468f-affc-011b35743b33.png)
    
7. As we are reading from a socket, we need to open a socket first. Open another EC2 terminal and allow __port 12345__ using the below __netcat__ command to communicate with Spark. We’ll send data from this session and Spark will read the input and write the output on the console:

    ````
    nc -l 12345
    ````

    ![image](https://user-images.githubusercontent.com/56078504/151303852-a38fa1d0-3a10-4912-a5ec-5104fcb037c6.png)

8. Switch to the first terminal and Run the application code using below command: 

    ````
    Spark2-submit read_from_socket.py
    ````
    ![image](https://user-images.githubusercontent.com/56078504/151303937-38697b45-c07a-4798-9dc4-93cb92810811.png)

9.	We can see now in below screenshot that spark is waiting for the input stream of data:

    ![image](https://user-images.githubusercontent.com/56078504/151303983-0e47db69-536a-4e23-8522-c972fd760478.png)

10. Let’s write “First Message” on other console and see the result on main console. We can see that the output is printed on another console. Below is the screenshot:

    ![image](https://user-images.githubusercontent.com/56078504/151304458-6eaf3ce5-26e4-47dd-ace4-daf6d65890dd.png)

11. Let’s write the “__Second Message__” and check the output on another console:

    ![image](https://user-images.githubusercontent.com/56078504/151304605-4ac3f1ee-70ad-4fb7-bade-015e602bebf4.png)

## Segment 2: Output Modes
The output modes of a streaming query define what DataFrames are to be written to the output sink. There are 3 output modes which are listed below:
- **Append -** Only modified records are added to the sink.
- __Update -__ Only modified records are added to the sink.
- __Complete -__ All records are added to the sink.

__Note:__ There are, however, a few restrictions on output modes. For example, no aggregations will happen with the update or append mode without watermarks. Also, if there are no aggregations in the streaming, the complete is not supported.

Let’s see how different output modes are implemented in Spark Structured Streaming application.

  ### Append:
  1. Run the below command to create a file __stream_output_modes.py__:
      ````
      vi stream_output_modes.py
      ````
      ![image](https://user-images.githubusercontent.com/56078504/151305747-ed431f39-80eb-4908-937b-64b265d692c7.png)

2.	Below is the complete code. Hit the ‘__I__’ key to turn the console into edit mode and paste the below codes to the file. Press ‘__Esc__’ key and enter __:wq!__ to save the file. Here output mode is set as append.

    ````
    from pyspark.sql import SparkSession

    spark = SparkSession  \
      .builder  \
      .appName("StructuredSocketRead")  \
      .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    lines = spark  \
      .readStream  \
      .format("socket")  \
      .option("host","localhost")  \
      .option("port",12345)  \
      .load()

    query = lines  \
      .writeStream  \
      .outputMode("append")  \
      .format("console")  \
      .start()

    query.awaitTermination()
    ````
    
    ![image](https://user-images.githubusercontent.com/56078504/151306019-060a3f24-3510-4267-9dc4-0f28d6792e9b.png)

3.	Open another EC2 terminal and allow port 12345 using the below netcat command:

    ````
    nc -l 12345
    ````

4.	Switch to the first terminal and run the application code using below command: 

    ````
    Spark2-submit stream_output_modes.py
    ````
    ![image](https://user-images.githubusercontent.com/56078504/151306176-7d1d50de-6a97-4df3-bcc4-6eb784bc61d5.png)

5.	Let’s write few messages on NetCat console and see the output on other console. Below is the screenshot:

    ![image](https://user-images.githubusercontent.com/56078504/151306210-af4f893f-23be-4029-976e-0a847593b9e3.png)

    As per the above output, we can see that each input is getting appended as new output with output mode set as __append__. First, we entered the input “__msg1__” which came as output in **Batch 0**. Next, we entered the input “__msg2__” which was printed in __Batch 1__. However, at the last, we inserted 2 inputs “__msg3__” and “__msg4__”, which are printed together in __Batch 2__. It shows that append mode is working properly.
    
    ### Complete Mode:
    
  


