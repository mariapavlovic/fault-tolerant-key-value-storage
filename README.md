# fault-tolerant-key-value-storage
fault-tolerant distributed system which stores key-value pairs across primary and backup nodes. Full linearizability achieved, tested with continuous get &amp; put operations of the key-value pairs through frequent node failures and recoveries, and port number reuse (of new backups using recently stale primary port)
