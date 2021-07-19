# Oracle module to load a huge number of geo-objects and then quickly searching through them
📝 Loading a huge number of geo-objects (in this case, base stations📡) in the fastest way 📈. Only delta is loaded. A continuous chain of the start and end dates of the object positioning on the coordinates is created.
***
## LOAD
> Only the delta and all new objects are loaded. In this case, the object ID is a group of values: id_src, lac, cell. 
> Also, when loading, the data is cleaned of garbage and thinned to a minimum set of rows, so that the history of changing coordinates for the geo object is preserved.

IN
id  | coordinates | begin | end
--- | ----------- | ------| ---
1  | 39.0, 50.0  | 2020-01-01 00:00:00  | 2022-01-01 00:00:00
1  | 39.0, 50.0  | 2020-03-01 00:00:00  | 2022-01-01 00:00:00
1  | 39.5, 50.5  | 2020-03-01 00:00:00  | 2022-01-01 00:00:00
1  | 39.5, 50.5  | 2020-03-05 00:00:01  | 2030-01-01 00:00:00

OUT
id  | coordinates | begin | end
--- | ----------- | ------| ---
1  | 39.0, 50.0  | 2020-01-01 00:00:00  | 2020-02-29 23:59:59
1  | 39.5, 50.5  | 2020-03-01 00:00:00  | 2030-01-01 00:00:00
* [Inizializaze](initialization.ddl) - create final table, log table and output types.
* Create package LOAD_GEO.
    * [source](load.pks)
    * [body](load.pkb)
* Execution examples
    * >     LOAD_GEO.LOAD_FILE(file_name in varchar2, only_dbms_out in number default null);
    * >     LOAD(only_dbms_out in number default null ); --load all files from directory

