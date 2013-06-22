package com.socrata.metrics

import org.junit.{Ignore, Test}
import com.socrata.metrics._
import java.util.Date

/**
 *
 */
class MigratorTest {
  @Test
  @Ignore
  def testMigrateView {
    val migration = Migrator.migrateView(ViewUid("1234xabcd"), ViewUid("43214xdcba"), DomainId(66), new Date(1234), new Date(4321))
    migration.foreach { x:MigrationOperation => println(x) }
  }
}