
TO DO still:
 * Eliminate duplicates either by
    1) writing to a text file and having some other process handle dups on import to db, or
    2) instead of dumping to db, update based on id
 * Read from data lake instead of local file
 * Write to Azure SQL instead of mysql
 * Read list of directories on data lake and call ETL function for each
 * Propagate "sensor" field down to each entity within a caliper nested record
     (Needed because the ID may be different from different sensors, and these
      need to be harmonized)
