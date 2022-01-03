# AWS-Web-Queue-Worker-Architecture

### by Hamza AIT BAALI

## How to Run

- First Clone this Repo including all submodules

### local Test without Using EC2

- Open Project two projects in eclipse
- Run WorkerMain first (workerMain has code that check if Queues are created if not it will create them for you)
- After that In **ClientMain** java file change:
  - bucket variable (optional) line:63
  - filename **(required)** line:64
  - filePath variable **(required)** line:65
  - outputPath variable **(required)** line:117
- After this Simply Run ClientWorker and wait until it finishes and you'll find the results in the S3 bucket and also in outputPath you choosed.


### Test Using an EC2

- Create an EC2 instance
- Log to the EC2 instance
- Install Java SDK
- export your AWS credentials in the EC2
  - export AWS_ACCESS_KEY_ID=*********************
  - export AWS_SECRET_ACCESS_KEY=****************************
- Download the worker-cloud-0.0.1-SNAPSHOT-jar-with-dependencies.jar file into the EC2 instance using github url or SFTP
- run ```screen``` Command to create a seprate session
  - If you run a program in SSH, and then close out ssh you can not get back into the console.That why I recommand you use screen to attach + detach a console
- Run the jar file
  - Command ```java -jar worker-cloud-0.0.1-SNAPSHOT-jar-with-dependencies.jar```
- To detach run: ctrl + a + d
- Close your terminal now if you want to
- clone Code from Github
- Open client-cloud code in Eclipse
- After that In **ClientMain** java file change:
  - bucket variable (optional) line:63
  - filename **(required)** line:64
  - filePath variable **(required)** line:65
  - outputPath variable **(required)** line:117
- After this Simply Run ClientWorker and wait until it finishes and you'll find the results in the S3 bucket and also in outputPath you choosed.
