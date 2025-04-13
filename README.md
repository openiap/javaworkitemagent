## Java workitem agent example
This is a example project, functions as a template for how to get started writing a more controlable workitem agent for the OpenCore platform.

We connect to the server, and also register an event listener waiting for "SignedIn" events. 
When we get a "SignedIn" event, register a message queue based of the `queue` environment variable, and start listening for messages.
This should match the name of the workitem queue we want this agent to handle.

When we get a message, we will pop a workitem of the workitem queue, and if one is found ( in case more agents are listening )
we start processing it inside `ProcessWorkitem` 
We then update the state of the workitem to successful or retry, depending on the outcome of `ProcessWorkitem`.

When running inside an agent make sure the `wiq` environment variable has been set to the name of the workitem queue you want to listen to.
When running local, make sure to add this to your .env file.
If you need to use a different queue name from the workitem queue name, you can set the `queue` environment variable to something different.


# Build and run

To build the project:
```bash
mvn clean package
```

To run the test application:
```bash
mvn package
java -jar target/javaworkitemagent-0.0.1-jar-with-dependencies.jar
```
github repository:
```
https://github.com/openiap/javaworkitemagent.git
```
