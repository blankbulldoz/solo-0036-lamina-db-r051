"""Independent state-machine verification for LaminaDB.

The package observes the public HTTP service and reconstructs the commit
history from the on-disk write-ahead log.  It never imports, links or reads
the Go executor and it does not consume the walkthrough demo's observations.
"""
