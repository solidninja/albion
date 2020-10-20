package is
package solidninja
package albion

/** Customisation of schema derivation or encoding/decoding.
  */
final case class Configuration(transformMemberNames: String => String = identity) {
  def withSnakeCaseMemberNames: Configuration = copy(
    transformMemberNames = renaming.snakeCase
  )

  def withKebabCaseMemberNames: Configuration = copy(
    transformMemberNames = renaming.kebabCase
  )
}

// taken from circe-derivation project
private[albion] object renaming {

  /** Snake case mapping */
  final val snakeCase: String => String = _.replaceAll(
    "([A-Z]+)([A-Z][a-z])",
    "$1_$2"
  ).replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase

  /** Kebab case mapping */
  val kebabCase: String => String =
    _.replaceAll(
      "([A-Z]+)([A-Z][a-z])",
      "$1-$2"
    ).replaceAll("([a-z\\d])([A-Z])", "$1-$2").toLowerCase

  final def replaceWith(pairs: (String, String)*): String => String =
    original => pairs.toMap.getOrElse(original, original)
}
