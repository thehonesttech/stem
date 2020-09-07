## Stem

Stem is an opinionated Event sourcing/CQRS framework that wants to avoid all the issues present in other frameworks.

Every choice has been made to avoid issues we had in the past. Protobuf is the protocol chosen in order to keep schema evolution for states,
events and Kafka.
The framework uses the amazing **ZIO** library for both streams and effect management.
This makes possible to have deterministic and quick tests.
 
This project is heavily inspired by **Lagom** and **Aecor**.