
### Question 1: What type of queues can you create? State their differences.

-  **Answer spesific to AWS SQS types**:

First-in, first-out (FIFO) and simple queues are two types of Amazon SQS queues. Message strings in FIFO queues are kept in the same sequence in which they were transmitted and received. FIFO queues may send, receive, and discard up to 300 messages per second. FIFO queues are used to communicate between applications in which the sequence of actions and events is important.

FIFO queues contain roughly the same functionality as ordinary queues, with the added benefits of providing ordering and exactly-once processing, as well as ensuring that messages are transmitted and received in the same sequence.

For simple queues message strings are kept in the same order as they were sent, but processing needs may cause the original order or sequence of messages to alter. simple queues can be used to batch messages for later processing or to distribute jobs over numerous worker nodes, for example.

  

-  **Answer for Queues in General**: There are four different types of queues:

	* Simple Queue : Insertion happens at the back of the line, while removal happens at the front. The FIFO (First in, First out) rule is carefully followed.

	* Circular Queue : The last entry in a circular queue points to the initial element, forming a circular connection. The key advantage of a circular queue over a simple queue is that it uses less memory. We can put an element in the first place if the last position is full and the first position is vacant. In a basic queue, this action is not feasible.

	* Priority Queue : A priority queue is a form of queue in which each piece has a priority assigned to it and is served according to that priority. If elements with the same priority appear in the queue, they are served in the order in which they appeared. Insertion is done according to the order in which the values arrive, and removal is done according to the order in which they are removed.

	* Double Ended Queue : Insertion and removal of items can be done from either the front or the back of a double-ended queue. As a result, it does not adhere to the FIFO (First In, First Out) principle.
  
--------

### Question 2: In which situations is a Web-Queue-Worker architecture relevant?

-  **Answer** :
	* Based on the documentation of Microsoft azure, this architecture style is For
		* Applications with a relatively simple domain.
		* Applications with some long-running workflows or batch operations.
		* When you want to use managed services, rather than infrastructure as a service (IaaS).

the common thing about these applications is that they have some resource-intensive tasks. which indicate whenever we have in an cloud app some tasks that are somewhat resource-intensive it's a good idea to start thinking about Web-Queue-Worker architecture to ease the load and avoid errors.