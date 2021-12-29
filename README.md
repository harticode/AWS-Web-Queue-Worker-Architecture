# AWS-Web-Queue-Worker-Architecture

### Hamza AIT BAALI

### Overview
--------

Assume you have been hired to automate the summarization of the hourly sales of a retailer with world wide presence. The sale transactions from everywhere in the world are registered in a unique ERP system. Because the retailer works 24/7, it is recommended not to perform the summarization in the same infrastructure as the ERP system to avoid performance degradation. Therefore, every hour the ERP system exports a comma separated values (CSV) file containing all the sale transactions carried out that hour.

Your task is to develop an application based on the *Web-Queue-Worker* architecture ([Microsoft, 2021](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/web-queue-worker)) to summarize these sale transactions in a Cloud environment. The retailer decided to use Amazon AWS as their cloud provider.

The application is divided in two main components: the **Client** and the **Worker**.

The **Client** is responsible for

1.  reading the CSV file

2.  upload it into the cloud

3.  send a message to the Worker signaling that there is a file ready to be processed

4.  wait until it receives a message from the Worker that the summarization was completed, and

5.  download the resulting file.

The **Worker** is responsible for

1.  wait for a message from the Client

2.  once the message is received with the name of the file to process, read the file

3.  calculate (a) the **Total Number of Sales**, (b) the **Total Amount Sold** and (c) the **Average Sold** per country and per product

4.  write a file in the cloud

5.  send a message with the name of the file to the Client

6.  wait for another message