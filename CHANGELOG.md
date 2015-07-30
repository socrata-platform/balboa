# Change Log

Document that is late to the game but very needed

## Version 0.16.8 - (Snapshot)

* Introduce Log depedendancy centralization via BalboaLogging.  Ensure that we are using SLF4J so that we can easily switch out logs under
the covers.  Currently we are using Log4J 1.2.  
* Added a Period Comparator so we can sort and order Period
* Cleaned package dependencies
* Made the dates within a Date Range immutable.