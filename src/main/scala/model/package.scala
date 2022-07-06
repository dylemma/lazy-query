import cats.Show
import scala.annotation.targetName
import scala.language.implicitConversions
import cats.syntax.show._

package object model {
	case class Identified[Id, A](id: Id, data: A)
	object Identified {
		implicit def unwrapId[A](identified: Identified[_, A]): A = identified.data
	}
	type #:[Id, A] = Identified[Id, A]
	extension[Id] (id: Id)
		@targetName("identifiedBy")
		def #:[A](data: A): Identified[Id, A] = new Identified(id, data)

	case class ProjectId(value: Int) extends AnyVal
	case class AnalysisId(value: Int) extends AnyVal

	case class Project(name: String)

}
