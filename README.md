# Oracle module to load a huge number of geo-objects and then quickly searching through them
📝 Loading a huge number of geo-objects (in this case, base stations📡) in the fastest way 📈. Only delta is loaded. A continuous chain of the start and end dates of the object positioning on the coordinates is created.
***
## LOAD
> Only the delta and all new objects are loaded. In this case, the object ID is a group of values: id_src, lac, cell. 
> Also, when loading, the data is cleaned of garbage and thinned to a minimum set of rows, so that the history of changing coordinates for the geo object is preserved.

DB TABLE

id  | coordinates | begin | end
| --- | ----------- | ------| ---
2  | 32.0, 48.0  | 1970-01-01 00:00:00  | 2020-02-29 23:59:59
2  | 32.0, 48.0  | 2020-03-01 00:00:00  | 2099-01-01 00:00:00

DATA TO LOAD 

id  | coordinates | begin | end
| --- | ----------- | ------| ---
1  | 39.0, 50.0  | 2020-01-01 00:00:00  | 2022-01-01 00:00:00
1  | 39.0, 50.0  | 2020-03-01 00:00:00  | 2022-01-01 00:00:00
1  | 39.5, 50.5  | 2020-03-01 00:00:00  | 2024-01-01 00:00:00
1  | 39.5, 50.5  | 2020-03-05 00:00:01  | 2030-01-01 00:00:00
2  | 31.0, 48.5  | 2020-03-02 00:00:00  | 2022-01-01 00:00:00
2  | 31.0, 48.5  | 2020-03-04 00:00:00  | 2022-01-01 00:00:00

DB TABLE AFTER LOAD

id  | coordinates | begin | end
| --- | ----------- | ------| ---
1  | 39.0, 50.0  | 1970-01-01 00:00:00  | 2020-02-29 23:59:59
1  | 39.5, 50.5  | 2020-03-01 00:00:00  | 2099-01-01 00:00:00
2  | 32.0, 48.0  | 1970-01-01 00:00:00  | 2020-03-01 23:59:59
2  | 31.0, 48.5  | 2020-03-02 00:00:00  | 2099-01-01 00:00:00

##### STEPS:
* Create final table, log table and others objects - [Initialization](initialization.ddl).
* Create package LOAD_GEO.
    * [source](load.pks)
    * [body](load.pkb)
* Execution examples
    * >     LOAD_GEO.LOAD_FILE('bs_2020_10_10_01_01_00_33', server_directory => '/mnt/bs', database_directory => 'BS_SRC', only_dbms_out => null);
    * >     LOAD(server_directory => '/mnt/bs', database_directory => 'BS_SRC', only_dbms_out => null); --load all files from directory

