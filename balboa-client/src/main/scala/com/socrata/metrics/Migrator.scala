package com.socrata.metrics


object Migrator {
   def migrateView(viewUid:String, destViewUid:String) {
       // Record the operations associated with all log*(viewUid) calls
       // Expand the operations to all time windows
       // Transform each operation into a read/write pair (with various other transformation)
       // Run the execution plan through an executor on the real data
   }
}
