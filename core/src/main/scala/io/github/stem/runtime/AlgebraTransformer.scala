package io.github.stem.runtime

import io.grpc.Status
import scalapb.zio_grpc.{TransformableService, ZTransform}
import io.github.stem.data.AlgebraCombinators
import io.github.stem.data.AlgebraCombinators.Combinators
import zio.{Tag, ZEnv, ZIO, ZLayer}
import zio.stream.ZStream

object AlgebraTransformer {
  private def layer[State: Tag, Event: Tag, Reject: Tag](algebra: AlgebraCombinators.Service[State, Event, Reject]) = ZLayer.succeed(algebra)

  private def transform[State: Tag, Event: Tag, Reject: Tag](algebra: AlgebraCombinators.Service[State, Event, Reject]) =
    new ZTransform[ZEnv with Combinators[State, Event, Reject], Status, ZEnv] {
      override def effect[A](io: ZIO[ZEnv with Combinators[State, Event, Reject], Status, A]): ZIO[ZEnv, Status, A] =
        io.provideCustomLayer(layer(algebra))

      override def stream[A](io: ZStream[ZEnv with Combinators[State, Event, Reject], Status, A]): ZStream[ZEnv, Status, A] =
        io.provideCustomLayer(layer(algebra))
    }
  def withAlgebra[State: Tag, Event: Tag, Reject: Tag, S[_, _]: scalapb.zio_grpc.TransformableService](
    service: S[ZEnv with Combinators[State, Event, Reject], Any],
    algebra: AlgebraCombinators.Service[State, Event, Reject]
  ): S[ZEnv, Any] = implicitly[TransformableService[S]].transform(service, transform[State, Event, Reject](algebra))

  object Ops {
    implicit class RichAlgebraService[State: Tag, Event: Tag, Reject: Tag, S[_, _]: scalapb.zio_grpc.TransformableService](service: S[ZEnv with Combinators[State, Event, Reject], Any]) {
      def withAlgebra(algebra:  AlgebraCombinators.Service[State, Event, Reject]): S[ZEnv, Any] = {
        AlgebraTransformer.withAlgebra(service, algebra)
      }
    }
  }

}
