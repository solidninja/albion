package is
package solidninja
package albion

import java.time.temporal.ChronoField
import java.time._

import scala.jdk.CollectionConverters._

import org.scalacheck.{Arbitrary, Gen}

/**
  *  Custom instances for date time generation, mainly to limit year range to [0, 9999]
  * @see https://github.com/47deg/scalacheck-toolbox/blob/master/datetime/src/main/scala/com/fortysevendeg/scalacheck/datetime/jdk8/GenJdk8.scala
  */
object ArbitraryTime {

  def genZonedDateTimeWithZone(maybeZone: Option[ZoneId] = None): Gen[ZonedDateTime] =
    for {
      year <- Gen.choose(1, 9999)
      month <- Gen.choose(1, 12)
      maxDaysInMonth = Month.of(month).length(Year.of(year).isLeap)
      dayOfMonth <- Gen.choose(1, maxDaysInMonth)
      hour <- Gen.choose(0, 23)
      minute <- Gen.choose(0, 59)
      second <- Gen.choose(0, 59)
      microOfSecond <- Gen.choose(0, 999999) // precision micros, 6 dp.
      zoneId <- maybeZone
        .map(Gen.const)
        .getOrElse(Gen.oneOf(ZoneId.getAvailableZoneIds.asScala.toList).map(ZoneId.of))
    } yield ZonedDateTime
      .of(year, month, dayOfMonth, hour, minute, second, microOfSecond * 1000, zoneId)

  implicit val arbitraryLocalDate: Arbitrary[LocalDate] = Arbitrary(genZonedDateTimeWithZone().map(_.toLocalDate))
  implicit val arbitraryLocalDateTime: Arbitrary[LocalDateTime] = Arbitrary(
    genZonedDateTimeWithZone().map(_.toLocalDateTime)
  )
  implicit val arbitraryLocalTime: Arbitrary[LocalTime] = Arbitrary(genZonedDateTimeWithZone().map(_.toLocalTime))

  // the encoding of instant aka timestamp seems to lose precision around the 5th dp, deal with it
  implicit val arbitraryInstant: Arbitrary[Instant] = Arbitrary(
    genZonedDateTimeWithZone()
      .map(_.toInstant)
      .map(i => i.`with`(ChronoField.NANO_OF_SECOND, (i.getNano / 100000L) * 100000L))
  )
}
