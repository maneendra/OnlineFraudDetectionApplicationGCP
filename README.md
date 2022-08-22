# Online Fraud Detection Platform
The online fraud detection platform is designed to handle real-time streaming data transaction data, process it immediately, and send the result to the relevant notification services.

The platform consists of microservices

 - PubSub
	 - Messaging service that ingest the transaction details to the system and send fraud status to the subscribed devices.
 - Dataflow
	 - The main services which connects all microservices together. It process the incoming transaction details, call relevant services, process the responses received and finally send the fraud status .
 - Rule Engine
	 - Rest web service hosted in compute engine and validate the transaction against five rules and send the transaction validation status to the Dataflow.
 - Machine Learning Model
	 - Machine learning model hosted in AI platform and send the fraud predection for the transaction details as the response when invoked the service.
 - Big Query
	 - Keep track of the transaction details and their status for the future reference.
