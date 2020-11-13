# Testing spark structured streaming
Unit Testing Apache Spark Structured Streaming using MemoryStream.
Unit testing Apache Spark Structured Streaming jobs using MemoryStream in a non-trivial task.In this post, therefore, I will show you how to start writing unit tests of Spark Structured Streaming.

###MemoryStream
MemoryStream is one of the streaming sources available in Apache Spark. This source allows us to add and store data in memory, which is very convenient for unit testing
This saves us a ton of time over running locally embedded Kafka.

We then create a streaming query that writes to a MemorySink. This allows us to test an end to end streaming query, without the need to Mock out the source and sink in our structured streaming application. This means you can plug in the tried and true DataSources from Spark and focus instead on ensuring that your application code is running hiccup free.

###Writing Spark Structured Streaming job

I am using a stringified JSON, that contains information I am normally receiving from my Kafka data source. You should use any data that fits your use case.

Application Configuration 
- config the sample events(app-config)
- provide checkpointing location
```
   value={"Organisation_Name":"City of York Council","Directorate":"Economy and Place","Department":"Transp Highways & Environment","Service_Plan":"Transport","Creditor_Name":"Road Safety Analysis Ltd","Payment_Date":"2/4/2020 0:00","Card_Transaction":"","Transaction_No":"202021CR00000001","Net_Amount":995,"Irrecoverable_VAT":"","Subjective_Group":"Supplies And Services","Subjective_Subgroup":"Grants and Subscriptions","Subjective_Detail":"Subscriptions"}   
   checkpointLocation=/data/checkpoint/real-time/payments
```
    