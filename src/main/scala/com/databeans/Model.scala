package com.databeans

case class TableStates(transaction_id: Int, tableName: String, initialVersion: Long, latestVersion: Long, isCommitted: Boolean)
