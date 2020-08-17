package stem.annotations

/**
  * Used to annotate method with a unique id (every method must have a different id) in order to allow evolution.
  * If not defined, renaming method will break the api during a deployment
  * @param id unique id sequential number
  */
class MethodId(id: Int) extends scala.annotation.StaticAnnotation
