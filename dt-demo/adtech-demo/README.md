### Description
This AdTech Demo reads CSV string messages containing ad information from a
configured Kafka topic, parses these strings to AdInfo objects using a custom csv schema
before filtering and enriching them.
Having the final enriched AdInfo events the application performs dimensional
computations specified in an event schema JSON and saves the results
to a HDHT store. Finally publish/subscribe query- and query-result operator use
the stored data to provide easily addable and customizable real-time visualisations.

Please see Dimensional Computations documentations for further details.
//TODO link

