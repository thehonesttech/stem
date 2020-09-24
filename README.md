## Stem

Stem is an opinionated Event sourcing/CQRS framework that wants to avoid all the issues present in other frameworks.

Every choice has been made to avoid issues we had in the past. Protobuf is the protocol chosen in order to keep schema evolution for states,
events and it is the serialization/deserialization protocol in Kafka.
The framework uses **ZIO** library for both streams and effects management.
This makes possible deterministic and quick tests.
 
This project is heavily inspired by **Lagom** and **Aecor**.
It has WIP quality, do not use in production yet.