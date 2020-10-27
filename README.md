## Stem

The key to building an elastic, resilient and responsive system is starting with the right architecture — one rooted in a solid understanding of microservices, including concepts and best practices. 
Stem is an opinionated Event sourcing/CQRS framework that allows you to adopt a Reactive architecture in an easy way.

The framework tries to lock on a specific technology only when we are sure it provides the best in class without possible drawbacks.

The framework uses **ZIO** library at its core for both streams and effects management.
This allows deterministic and quick tests.
 
This project is heavily inspired by **Lagom** and **Aecor**.
It has WIP quality, do not use in production yet.

The **example** project contains code and test of a possible implementation of a basic ledger
Below some features of the library/framework are described.

####RPC style Entities
DDD Entities in Stem, usually called Stemtities, use Akka (Cluster) and a macro in order to
allow RPC style invocation. Amount of boilerplate code is drastically reduced and an entity can be invoked
as a normal class.
Testing a Stemtity is a lot easier since it can be tested like normal code.

The library will distribute the request in the cluster and serialize commands using either Scodec or Protobuf.

The optional annotation `@MethodId` can be used to maintain schema compatibility if method is renamed.
The id used will be the unique number set in the annotation.

####GRPC services
One of the problem we had with **Lagom** was the schema evolution bit.
Schema evolution allows to evolve your models (that needs to be persisted) without breaking deployments.
This is a very important feature in a running system and we believe it should be a first class concern.
Lagom gives freedom of choice in the ways we could evolve the schema (with preference for JSON) but we found
it to be a problematic part of the framework.

Sometimes, it is easy to choose a code first approach using Avro4s or Json macros but, in our opinion, it is too fragile
and it is pretty easy to break the tests or the system

For this reason, we believe that a schema first technology should be used in order to avoid the fragility of 
changing code breaking integration tests too late in the deployment process.

Schema evolution and great performance are the reason we chose Grpc (with ZIO-Grpc) in order to provide a P2P microservices way of communication
 
####ZIO
