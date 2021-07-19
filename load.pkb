CREATE OR REPLACE PACKAGE BODY LOAD_GEO AS 
--===========================================================================================================================================================================
    PROCEDURE LOAD_FILE(file_name in varchar2, server_directory in varchar2 default '/mnt/bs', database_directory in varchar2 default 'BS_SRC', only_dbms_out in number default null) AS
        int_id_oper_dest NUMBER;
        int_id_oper_src NUMBER;
        g_date_start date := SYSDATE;
        parallel_ number:=8;
        old_grp		varchar2(300);

        date_new_file_bs DATE;
        date_last_load_bs DATE;

        date_max DATE DEFAULT TO_DATE('2099-01-01 00:00:00', 'YYYY-MM-dd hh24:mi:ss');
        date_min DATE DEFAULT TO_DATE('1970-01-01 00:00:00', 'YYYY-MM-dd hh24:mi:ss');
        --========================================================================================================= 
        --========================================================================================================= 
        PROCEDURE L_LOGGER(message in varchar2, filename in varchar2, id_dest in number default null, id_src in number default null, is_error in varchar2 default null, only_dbms_output in number default null ) is
            pragma autonomous_transaction;
        BEGIN
            if only_dbms_output is not null then
                dbms_output.put_line(SYSDATE||' '||filename|| (case when is_error is not null then '[ '||is_error||' ]' else '' end)  ||' =>'||id_dest||' =>'||id_src||'  => '||message);
            else
                INSERT INTO BS_SRC_log values( src_log.nextval, SYSDATE, filename, id_dest, id_src, is_error, message,g_date_start );
                commit;
            end if;
        END;
        --========================================================================================================= 
        PROCEDURE CHECK_VALID_NAME_FILE(file_name in varchar2, id_oper_dest out number,  id_oper_src out number, date_new_file_bs out date, only_dbms_out in number default null) IS
            exist_id_oper_src NUMBER;
            year_ varchar2(10);
            month_ varchar2(10);
            day_ varchar2(10);
            hours_ varchar2(10);
            minute_ varchar2(10);
            sec_ varchar2(10);
            exist_bs_oper_src_table NUMBER;
            date_last_load_bs date;
            int_id_oper_src number;
            int_id_oper_dest number;
        BEGIN

            select count(1) into exist_bs_oper_src_table from user_tables t where t.table_name = 'BS_oper_src';
            if exist_bs_oper_src_table = 0 then
                RAISE_APPLICATION_ERROR(-20010, 'Table BS_oper_src is not exist. Impossible continue work.');
            end if;

            --ниже происходит валидация имени файла. Имя файла должно начинаться на bs
            --пример имени файла - bs_2021_07_15_17_49_11_46
            if file_name not like 'bs%_%_%_%' then
                RAISE_APPLICATION_ERROR(-20000, 'Not valid begin name file = ' || file_name || '. Must be "bs"');
            end if;

            date_new_file_bs := to_date(substr(file_name,4,19),'yyyy_mm_dd_hh24_mi_ss');
            int_id_oper_src := to_number(substr('bs_2021_07_15_17_49_11_46',24)) ;

            --пропускаем определенные источники, т.к. дублируют данные или иное
            if int_id_oper_src in (2, 30, 6, 18) then
                RAISE_APPLICATION_ERROR(-20102, 'Pass file, oper src = ' || int_id_oper_src || ' of name file = ' || file_name || '');
            end if;

            SELECT COUNT(1) INTO exist_id_oper_src FROM BS_oper_src t WHERE t.id_oper_src = int_id_oper_src;
            --ниже происходит валидация полученного id oper
            if exist_id_oper_src = 0 then
                RAISE_APPLICATION_ERROR(-20101, 'Got id oper src = ' || int_id_oper_src || ' of name file = ' || file_name || ' not exist in table BS_oper_src');
            end if;

            --если родитель берем максимальную дату загрузки всех родителей по оператору,если дочка, то только дату дочки
            select t.id_oper_dest, max(t.DATE_LOAD_BS) into int_id_oper_dest,date_last_load_bs from BS_oper_src t 
            where t.id_oper_src in(
                SELECT
                    t2.id_oper_src
                FROM
                    bs_oper_src t,bs_oper_src t2 
                where t.id_oper_src = int_id_oper_src
                and t.id_oper_dest = t2.id_oper_dest
                and 
                case 
                    when t.is_parent = 1 and t2.is_parent =1 then 1 
                    when t.is_parent = 0 and t.id_oper_src = t2.id_oper_src then 1
                    else 0 
                end =1
            ) group by t.id_oper_dest;
            --ниже происходит валидация полученной даты из имени файла.
            if date_last_load_bs is not NULL and date_last_load_bs >= date_new_file_bs then
                RAISE_APPLICATION_ERROR(-20100, 'Not valid file date = ' || date_new_file_bs || '. Because last load bs was ' || date_last_load_bs || ' | id_dest = '||int_id_oper_dest|| ', id_src = '||int_id_oper_src);
            end if;

            id_oper_dest:= int_id_oper_dest;
            id_oper_src:= int_id_oper_src;

            L_LOGGER(message => 'Validate filename complete',  filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

        END CHECK_VALID_NAME_FILE;
        --========================================================================================================= 
        PROCEDURE UPDATE_STATISTIC as
            exist_bs_oper_src_table NUMBER;
            sel varchar2(32000);
        BEGIN
            select count(1) into exist_bs_oper_src_table from user_tables t where t.table_name = 'BS_oper_src';
            if exist_bs_oper_src_table = 0 then
                RAISE_APPLICATION_ERROR(-20010, 'Table BS_oper_src is not exist. Impossible continue work.');
            end if;

            sel := 'UPDATE BS_oper_src
                        SET DATE_LOAD_BS = :date_load_bs
                        WHERE ID_oper_src = :oper_id_src';

            execute immediate sel using date_new_file_bs, int_id_oper_src;
            commit;
            L_LOGGER(message => 'Update statistic', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );
        END;
        --====================================================================================================
        PROCEDURE CREATE_EXTERNAL_TABLE(file_name in varchar2) AS
            exist_ext_table NUMBER;
            sel varchar2(32000);
        BEGIN
            select count(1) into exist_ext_table from user_tables t where t.table_name = 'EXT_BS_SRC';
            if exist_ext_table != 0 then
                execute immediate 'drop table EXT_bs_final purge';
            end if;
            sel := 'CREATE TABLE EXT_bs_final (
                        ...
                        lac NUMBER NULL ,
                        cell NUMBER NULL ,
                        latitude VARCHAR2(20) NULL,
                        longitude VARCHAR2(20) NULL
                    ) 
                    ORGANIZATION EXTERNAL ( 
                        TYPE ORACLE_LOADER
                        DEFAULT DIRECTORY '||database_directory||'
                        ACCESS PARAMETERS ( 
                            records delimited by newline 
                            characterset UTF8
                            badfile bs_bad:''BS_SRC.bad''
                            logfile bs_log:''BS_SRC.log''
                            date_cache 100000
                            fields terminated by ''|''
                            ltrim missing field values are null
                            reject rows with all null fields (
                                ...
                                ,lac
                                ,cell
                                ,latitude
                                ,longitude
                            ) 
                        )
                        LOCATION ('''||file_name||''')
                    )
                    REJECT LIMIT 0
                    parallel 4';
            execute immediate sel;

            L_LOGGER(message => 'Create external table', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

        END CREATE_EXTERNAL_TABLE;
        --====================================================================================================
        PROCEDURE EXTERNAL_TO_TEMP AS
            exist_temp_table NUMBER;
            count_load NUMBER;
            sel varchar2(32000);
        BEGIN
            select count(1) into exist_temp_table from user_tables t where t.table_name = 'TEMP_BS_SRC';

            if exist_temp_table = 1 then
                execute immediate 'drop table TEMP_bs_final purge';
            end if;
            SELECT t.id_oper_dest INTO int_id_oper_dest FROM bs_oper_src t where t.id_oper_src = int_id_oper_src;

            sel := 'CREATE /*+ parallel('||parallel_||')*/  TABLE TEMP_bs_final tablespace bs_temp AS 
                        select /*+ parallel('||parallel_||')*/ 
                                ...
                                , TO_NUMBER(REPLACE(t2.latitude, ''.'', '','')) as latitude
                                , TO_NUMBER(REPLACE(t2.longitude, ''.'', '','')) as longitude
                                , t2.lac
                                , t2.cell
                        from ext_bs_final t2';         
            execute immediate sel;
            commit;

            sel := 'select count(1) from TEMP_BS_SRC';
            execute immediate sel into count_load;

            L_LOGGER(message => 'Create TEMP table, '||count_load||' rows ', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

        END EXTERNAL_TO_TEMP;
        --====================================================================================================    
        PROCEDURE DEL_PARENTS_BS_IN_CHILD_FILE  AS
            --На первом шаге обработки мы выявляем новые пары lac, cell, которые пришли в файлах и их нет в таблице bs_final у их родителей
            int_is_parent NUMBER;
            exist_temp_table1 NUMBER;
            count_rows NUMBER;
            sel varchar2(32000);
        BEGIN
            select count(1) into exist_temp_table1 from user_tables t where t.table_name = 'TEMP_BS_SRC_PRE';

            if exist_temp_table1 = 1 then
                execute immediate 'DROP table TEMP_BS_SRC_PRE purge';
            end if;

            select t.is_parent into int_is_parent from bs_oper_src t where t.id_oper_src = int_id_oper_src;
            if int_is_parent = 0 then
                L_LOGGER(message => 'Oper is child', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

                --Находим строчки у дочерних операторов, если они имеются у их родителя
                sel := 'CREATE TABLE /*+ parallel('||parallel_||')*/ TEMP_BS_SRC_PRE tablespace bs_temp AS 
                            SELECT * FROM TEMP_bs_final t2
                                where (t2.lac, t2.cell) not in(
                                            SELECT /*+ parallel('||parallel_||')*/distinct b.lac, b.cell 
                                                FROM bs_final b
                                                WHERE b.OPER_id_src IN (SELECT bo.ID_oper_src FROM BS_oper_src bo WHERE bo.ID_oper_dest = ' || int_id_oper_dest || ' AND bo.IS_PARENT = 1)
                                                    or  (b.OPER_id_src is null  and b.OPER_id_dest = ' || int_id_oper_dest || ')
                                                )
                                                    ';
                execute immediate sel;
                commit;
                L_LOGGER(message => 'Delete parents BS in child file, '||count_rows||' rows out', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

            elsif int_is_parent = 1 then 
                L_LOGGER(message => 'Oper is parent', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );
                execute immediate 'alter table BS.TEMP_bs_final rename to TEMP_BS_SRC_PRE';
            else
                RAISE_APPLICATION_ERROR(-20004, 'Value is_parent = ' || int_is_parent || ' in table bs_oper_src dont right! Must be 0 or 1');
            end if;

            sel := 'select count(1) from TEMP_BS_SRC_PRE';
            execute immediate sel into count_rows;

        END DEL_PARENTS_BS_IN_CHILD_FILE;
        --====================================================================================================    
        PROCEDURE DELETE_BAD_BS AS
            exist_filter_table NUMBER;
            count_rows_history NUMBER;
            sel varchar2(32000);

            PROCEDURE FILTER(temp_table_name in varchar2, type_filter in varchar2) AS
                exist_temp_table number;
                exist_filter_table number;

            BEGIN
                select count(1) into exist_temp_table from user_tables t where t.table_name = temp_table_name;
                if exist_temp_table = 0 then
                    RAISE_APPLICATION_ERROR(-20005, 'PROCEDURE FILTER ERROR. TEMP_TABLE NAME = ' || temp_table_name || ' NOT EXIST IN SCHEMA');
                end if;

                select count(1) into exist_filter_table from user_tables t where t.table_name = 'BS_SRC_FILTER';
                if exist_filter_table = 0 then
                    RAISE_APPLICATION_ERROR(-20006, 'PROCEDURE FILTER ERROR. FILTER_TABLE NAME = BS_SRC_FILTER NOT EXIST IN SCHEMA');
                end if;

                if type_filter = 'filter_bad_coordinate' then

                    sel := 'INSERT   INTO BS_SRC_FILTER 
                                SELECT/*+ parallel('||parallel_||')*/ ''delete_bad_coordinate'', rowid 
                                    FROM '|| temp_table_name || ' t 
                                    WHERE (t.latitude is null or t.longitude is null) ';

                    execute immediate sel;
                    L_LOGGER(message => 'Create filter "delete_bad_coordinate", rows '||sql%rowcount, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

                elsif type_filter = 'filter_bad_lac_cell' then
                    sel := 'INSERT  INTO BS_SRC_FILTER 
                                SELECT/*+ parallel('||parallel_||')*/ ''filter_bad_lac_cell'', rowid 
                                    FROM '|| temp_table_name || ' t 
                                    WHERE (t.lac is null or t.cell is null) or (t.lac = 0 or t.cell = 0) or (t.lac = '''' or t.cell = '''')';

                    execute immediate sel;
                    L_LOGGER(message => 'Create filter "filter_bad_lac_cell", rows '||sql%rowcount, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

                end if;

            END;

        BEGIN
            select count(1) into exist_filter_table from user_tables t where t.table_name = 'BS_SRC_FILTER';
            if exist_filter_table = 0 then
                RAISE_APPLICATION_ERROR(-20008, 'Procedure CREATE_BAD_BS_ROWID_LIST error. Table BS_SRC_FILTER not exist');
            end if;

            sel := 'DELETE /*+ parallel('||parallel_||')*/ FROM TEMP_BS_SRC_PRE t
                    WHERE 
                    (t.latitude is null or t.longitude is null)
                    OR  (
                        wire_cell is null
                        AND (
                            (t.lac is null or t.cell is null) 
                            or (t.lac = 0 or t.cell = 0) 
                        )
                    )' ;
            execute immediate sel;

            L_LOGGER(message => 'Delete bad bs meta, '||sql%rowcount||' rows', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );
            commit;

            sel :='insert into BS_SRC_bad 
                    select t.*,:file_name,:date_load from TEMP_BS_SRC_PRE t
                        where (lac, cell,begin_date_src,end_date_src) in(
                            select lac, cell,begin_date_src,end_date_src from(


                                select /*+ parallel('||parallel_||')*/ lac, cell,t.begin_date_src,t.end_date_src,  
                                dense_rank() OVER (PARTITION BY lac, cell,begin_date_src,end_date_src  ORDER BY address,latitude,longitude) dr,
                                dense_rank() OVER (PARTITION BY lac, cell  ORDER BY begin_date_src desc,end_date_src desc) dr_laccell
                                from TEMP_BS_SRC_PRE t 
                            )  where dr > 1 and dr_laccell=1
                        )';
            execute immediate sel using file_name, SYSDATE;
            L_LOGGER(message => 'Insert into bad bs non-consistent, '||sql%rowcount||' rows', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

            commit;
        END DELETE_BAD_BS;
        --====================================================================================================      
        PROCEDURE CREATE_NEW_BS_SET AS
            --создаем таблицу чтобы потом проверять факт того новая бс или нет (только lac cell)
            exist_table_res_3 NUMBER;
            sel varchar2(32000);
            count_ number;
        BEGIN
            select count(1) into exist_table_res_3 from user_tables t where t.table_name = 'TEMP_BS_SRC_RES_3';
            if exist_table_res_3 = 1 then
                execute immediate 'DROP table TEMP_BS_SRC_RES_3 purge';
            end if;

            sel := 'CREATE TABLE TEMP_BS_SRC_NEW_BS_SET tablespace bs_temp AS
                        select distinct lac, cell from TEMP_BS_SRC_PRE MINUS
                        SELECT distinct lac,cell FROM  bs_final where oper_id_dest='||int_id_oper_dest||'';
            execute immediate sel;

            execute immediate 'select count(1) from TEMP_BS_SRC_NEW_BS_SET' into count_;
            L_LOGGER(message => 'Create new bs SET, rows '||count_, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

        END CREATE_NEW_BS_SET;
        --====================================================================================================

        PROCEDURE DELETE_BEGIN_DATE_COLLISIONS AS
            exist_temp_table2 NUMBER;
            count_rows_history NUMBER;
            sel varchar2(32000);

        BEGIN
            select count(1) into exist_temp_table2 from user_tables t where t.table_name = 'TEMP_BS_SRC_2';

            if exist_temp_table2 = 1 then
                execute immediate 'DROP table TEMP_BS_SRC_2 purge';
            end if;

            sel := 'CREATE TABLE  /*+ parallel('||parallel_||')*/ TEMP_BS_SRC_2 tablespace bs_temp AS
                        SELECT  /*+ parallel('||parallel_||')*/ t2.*   
                        FROM (
                                SELECT /*+ parallel('||parallel_||')*/t.*, ROW_NUMBER() OVER (PARTITION BY lac, cell,t.begin_date_src ORDER BY t.end_date_src desc,  t.address, t.latitude, t.longitude, t.station_type, t.azimut, t.diagramm, t.frequency_total, t.generation, t.cell_type) rn_date_collision
                                    FROM TEMP_BS_SRC_PRE t 
                             ) t2 

                        WHERE rn_date_collision = 1' ;

            execute immediate sel;
            commit;
            sel := 'select count(1) from TEMP_BS_SRC_2';
            execute immediate sel into count_rows_history;

            execute immediate 'alter table BS.TEMP_BS_SRC_PRE rename to TEMP_BS_SRC_PRE_3';
            execute immediate 'alter table BS.TEMP_BS_SRC_2 rename to TEMP_BS_SRC_PRE';

            L_LOGGER(message => 'Delete begin_date collisions, '||count_rows_history||' rows out', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

        END DELETE_BEGIN_DATE_COLLISIONS;
        --====================================================================================================    
        PROCEDURE DETACH_DATA_TO_PERIOD_LOAD AS
            exist_temp_table2 NUMBER;
            count_rows_history NUMBER;
            sel varchar2(32000);
            tab_colls varchar2(4000);
            coll_create_sh1 varchar2(1000):='dbms_crypto.Hash(utl_raw.cast_to_raw(
                                            t.address||''_''||
                                            t.latitude||''_''||
                                            t.longitude||''_''||
                                            t.station_type||''_''||
                                            t.azimut||''_''||
                                            t.diagramm||''_''||
                                            t.frequency_total||''_''||
                                            t.generation||''_''||
                                            t.cell_type),3
                                        )';
        BEGIN
            select count(1) into exist_temp_table2 from user_tables t where t.table_name = 'TEMP_BS_SRC_2';

            if exist_temp_table2 = 1 then
                execute immediate 'DROP table TEMP_BS_SRC_2 purge';
            end if;

            select LISTAGG('t.'||column_name, ', ') WITHIN GROUP (order by column_id) colls  
                into tab_colls from user_tab_columns where table_name='TEMP_BS_SRC_PRE';
            --выделяем две большие сущности: дельта больше текущей + новые БС (new_bs)
            --в свою очередб дельта больше текущей разбивается на два случая: b>b, b=b,e>e(при этом условии проверяем чтоб текущий максимальный sh1 по lca cell был другим)
            sel := 'CREATE TABLE /*+ parallel('||parallel_||')*/ TEMP_BS_SRC_2 tablespace bs_temp AS
                    select /*+ parallel('||parallel_||')*/ load_type, sh1, begin_date_from_final, begin_date_src_from_final, '||tab_colls||' from(
                        select
                            case 
                            when t.begin_date_src = t2.begin_date_src then ''b=b,e>e''
                            when t.begin_date_src > t2.begin_date_src then ''b>b''
                            else ''unknown''
                            end load_type,
                            '||coll_create_sh1||' sh1,
                            t.* , t2.sh1 as t2sh1,
                            t2.begin_date as begin_date_from_final, t2.begin_date_src as begin_date_src_from_final
                        from TEMP_BS_SRC_PRE t, bs_final t2
                            where t.lac = t2.lac and t.cell = t2.cell and t2.oper_id_dest='||int_id_oper_dest||' and t2.end_date=to_date('''||to_char(date_max,'ddmmyyyy')||''',''ddmmyyyy'')
                            and (
                                t.begin_date_src > t2.begin_date_src
                                or (
                                    t.BEGIN_DATE_src = t2.begin_date_src and t.end_date_src > t2.end_date_src 
                                )
                            )
                    ) t where case when load_type=''b=b,e>e'' and sh1=t2sh1 then 0 else 1 end = 1
                    union all
                    select /*+ parallel('||parallel_||')*/ ''new_bs'' as load_type,'||coll_create_sh1||' sh1,null,null, t.* from TEMP_BS_SRC_PRE t
                        where (lac,cell) in (select * from TEMP_BS_SRC_NEW_BS_SET)' ;

            execute immediate sel;
            commit;

            select LISTAGG('t.'||column_name, ', ') WITHIN GROUP (order by column_id) colls  
                into tab_colls from user_tab_columns where table_name='TEMP_BS_SRC_PRE';
            --максимальная строка по lac cell, не попавшая в предыдущую выборку
            sel:='CREATE TABLE /*+ parallel('||parallel_||')*/ TEMP_BS_SRC_3 tablespace bs_temp AS
                    select /*+ parallel('||parallel_||')*/ ''max'' as load_type, '||coll_create_sh1||' sh1, '||tab_colls||' from(
                        select /*+ parallel('||parallel_||')*/ t.*, ROW_NUMBER() OVER (PARTITION BY lac, cell ORDER BY t.BEGIN_DATE_src desc, t.END_DATE_src desc) rn_hist 
                        from TEMP_BS_SRC_PRE t
                        where (lac,cell) not in(
                            select lac,cell from TEMP_BS_SRC_2
                        )
                    ) t
                    where  rn_hist = 1';
            execute immediate sel;
           
            sel:='insert into TEMP_BS_SRC_2
                    select t.load_type, t.sh1,   t2.begin_date as begin_date_from_final, t2.begin_date_src as begin_date_src_from_final, '||tab_colls||' 
                    from TEMP_BS_SRC_3 t, bs_final t2
                        where t2.oper_id_dest=:id_oper_dest and t.lac=t2.lac and t.cell=t2.cell and t2.end_date=:date_max
                        and t.sh1!=t2.sh1';        
            execute immediate sel using int_id_oper_dest, date_max;

            sel := 'select count(1) from TEMP_BS_SRC_2';
            execute immediate sel into count_rows_history;

            execute immediate 'alter table BS.TEMP_BS_SRC_PRE rename to TEMP_BS_SRC_PRE_4';
            execute immediate 'alter table BS.TEMP_BS_SRC_2 rename to TEMP_BS_SRC_PRE';

            L_LOGGER(message => 'Detach data to PERIOD LOAD, '||count_rows_history||' rows out', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

            commit;
        END DETACH_DATA_TO_PERIOD_LOAD;
        --====================================================================================================    
        PROCEDURE CALC_DATE_RANGE AS
            sel varchar2(32000);
            tab_colls varchar2(4000);
            count_rows_history number;
        BEGIN

            select LISTAGG('t.'||column_name, ', ') WITHIN GROUP (order by column_id) colls  
                into tab_colls from user_tab_columns where table_name='TEMP_BS_SRC_PRE';

            --проставляем непрерывную цепочку дат для каждой пары lac,cell + удаляем повторяющиеся БС в цепочке если sh1 равны(осавляем только крайние, пример  - было 111122113, станет 1122113, 1-sh1,2-sh2 и т.д. )
            sel:='CREATE /*+ parallel('||parallel_||')*/ TABLE  TEMP_BS_SRC_2 tablespace bs_temp AS
                    select
                        case 
                            when begin_date_src_prev is null and load_type=''b>b'' then BEGIN_DATE_src
                            when begin_date_src_prev is null and load_type in(''max'',''b=b,e>e'') then BEGIN_DATE_from_final
                            when begin_date_src_prev is null then to_date('''||to_char(date_min,'ddmmyyyy')||''',''ddmmyyyy'')
                            else BEGIN_DATE_src
                        end BEGIN_DATE,
                        case 
                            when begin_date_src_NEXT is null then to_date('''||to_char(date_max,'ddmmyyyy')||''',''ddmmyyyy'')
                            else begin_date_src_NEXT - INTERVAL ''1'' second
                        end END_DATE,
                        t.* 
                    from(
                        SELECT /*+ parallel('||parallel_||')*/
                        (lag(t.BEGIN_DATE_src) over (partition by lac,cell order by lac,cell, t.BEGIN_DATE_src)) as begin_date_src_prev,
                        (lead(t.BEGIN_DATE_src) over (partition by lac,cell order by lac,cell, t.BEGIN_DATE_src)) as begin_date_src_NEXT,
                           t.*
                        FROM(
                            select 
                                (lag(t.sh1) over (partition by lac,cell order by lac,cell, t.BEGIN_DATE_src)) as sh1_prev,
                                (lead(t.sh1) over (partition by lac,cell order by lac,cell, t.BEGIN_DATE_src)) as sh1_NEXT,
                                t.*
                            from  TEMP_BS_SRC_PRE t
                        ) t
                        where (sh1_prev is null or sh1_prev!=sh1) or (sh1_NEXT is null or sh1_NEXT!=sh1) 
                    ) t
                    ';
            execute immediate sel;

            sel := 'select count(1) from TEMP_BS_SRC_2';
            execute immediate sel into count_rows_history;

            --сохраняем уже загруженный в финальную таблицу bs_final begin_date_src( begin_date уже заполнен и равен ему,код выше ), чтобы сохранить логику последующей загрузки периода
            sel := 'UPDATE /*+ parallel('||parallel_||')*/TEMP_BS_SRC_2
                    set begin_date_src = begin_date_src_from_final
                        where load_type in(''max'',''b=b,e>e'')';
            execute immediate sel;

            execute immediate 'alter table BS.TEMP_BS_SRC_PRE rename to TEMP_BS_SRC_PRE_5';
            execute immediate 'alter table BS.TEMP_BS_SRC_2 rename to TEMP_BS_SRC_PRE';

            L_LOGGER(message => 'Calculate date ranges, '||count_rows_history||' rows out', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

            commit;

        END CALC_DATE_RANGE;
        --====================================================================================================       
        PROCEDURE MERGE_PERIOD_BS_FINAL AS
            sel varchar2(32000);
            tab_colls varchar2(4000);
            count_rows_history number;
        BEGIN
            select LISTAGG('t.'||column_name, ', ') WITHIN GROUP (order by column_id) colls  
                into tab_colls from user_tab_columns where table_name='BS_SRC';
            -- указываем хинт noparallel, чтобы в случае ошибки могли успешно сделать ROLLBACK

            --max и b=b,e>e
            sel := 'INSERT /*+ noparallel('||parallel_||')*/ INTO BS_SRC_ARCH
                    select t.*, SYSDATE from bs_final t
                        where ( cell,lac,oper_id_dest,end_date) in (
                            select cell,lac,:oper_id_dest, :end_date from TEMP_BS_SRC_PRE
                                where load_type in (''max'',''b=b,e>e'')
                    )';
            execute immediate sel using int_id_oper_dest, date_max;

            sel := 'DELETE /*+ noparallel*/ from bs_final 
                        where ( cell,lac,oper_id_dest,end_date) in (
                            select cell,lac,:oper_id_dest, :end_date from TEMP_BS_SRC_PRE
                                where load_type in (''max'',''b=b,e>e'')
                    )';
            execute immediate sel using int_id_oper_dest, date_max;
            L_LOGGER(message => 'Insert into archive and delete '||sql%rowcount||' rows processed', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );
            --/

            --b>b
            sel := 'merge /*+ noparallel*/ into bs_final t
                    using(
                        select lac,cell,  begin_date - INTERVAL ''1'' second as new_date_end_act from TEMP_BS_SRC_PRE
                            where load_type=''b>b'' and begin_date_src_prev is null
                    ) f
                    on (t.cell= f.cell and t.lac=f.lac and t.oper_id_dest=:int_id_oper_dest )
                    when matched then update set t.end_date = f.new_date_end_act where  t.end_date=:date_max
                    ';
            execute immediate sel using int_id_oper_dest, date_max;
            L_LOGGER(message => 'Update actual bs end_date at type b>b '||sql%rowcount||' rows processed', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );
            --/

            sel := 'INSERT /*+ noparallel*/ INTO BS_SRC
                    select '||tab_colls||' from TEMP_BS_SRC_PRE t';
            execute immediate sel;
            L_LOGGER(message => 'Merge completed, '||sql%rowcount||' rows inserted', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

            commit;
        END MERGE_PERIOD_bs_final;
        --====================================================================================================        
        PROCEDURE CLEAR_TEMP_DATA AS
            type arr_class is table of  varchar2(50);
            arr arr_class := arr_class(
                'TEMP_BS_SRC', 
                'TEMP_BS_SRC_PRE',
                'TEMP_BS_SRC_PRE_1',
                'TEMP_BS_SRC_PRE_2',
                'TEMP_BS_SRC_PRE_3',
                'TEMP_BS_SRC_PRE_4',
                'TEMP_BS_SRC_PRE_5',
                'TEMP_BS_SRC_2',
                'TEMP_BS_SRC_3',
                'TEMP_BS_SRC_RES',
                'TEMP_BS_SRC_RES_1',
                'TEMP_BS_SRC_RES_2',
                'TEMP_BS_SRC_RES_3',
                'TEMP_BS_SRC_NEW_BS_SET'
            );
        BEGIN
            for i in arr.first..arr.last loop
                begin
                    execute immediate 'DROP TABLE '||arr(i)||' purge';
                exception when others then null;
                end;    
            end loop;

            begin
                execute immediate 'truncate table BS_SRC_FILTER';
            exception when others then null;
            end;

            L_LOGGER(message => 'All temp data was cleared!', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );
        END CLEAR_TEMP_DATA;
        --====================================================================================================
        PROCEDURE DEL_FILE(v_fname in varchar2) is
            l_fexists boolean;
            l_file_Length pls_integer;
            l_block_size pls_integer;
        BEGIN
            -- java
            rc('/bin/rm -f '||server_directory||' '||file_name);

            begin       
                UTL_FILE.FREMOVE( 'BS_SRC' , convert(v_fname,'UTF8') );
            exception when others then 
                null;
            end;

            begin
                utl_file.fgetattr('BS_SRC', v_fname, fexists => l_fexists, file_Length => l_file_Length, block_size => l_block_size);
                if(l_fexists) then
                    RAISE_APPLICATION_ERROR(-20120, 'File '||file_name||' do not removed');
                end if;	
            end;

            L_LOGGER(message => 'File deleted', filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out );

        END DEL_FILE;
        --====================================================================================================

    BEGIN
    
        CHECK_VALID_NAME_FILE(file_name, int_id_oper_dest , int_id_oper_src, date_new_file_bs, only_dbms_out);
        CLEAR_TEMP_DATA();
        CREATE_EXTERNAL_TABLE(file_name);
        EXTERNAL_TO_TEMP();
        DEL_PARENTS_BS_IN_CHILD_FILE();
        DELETE_BAD_BS();

        -- Загрузка всего периода данных по паре lac,cell        
        CREATE_NEW_BS_SET(); 
        DELETE_BEGIN_DATE_COLLISIONS();
        DETACH_DATA_TO_PERIOD_LOAD(); 
        CALC_DATE_RANGE();
        MERGE_PERIOD_BS_FINAL();

        UPDATE_STATISTIC();
        DEL_FILE(file_name);
        CLEAR_TEMP_DATA();

    EXCEPTION 
        WHEN file_date_less_then_act then
            L_LOGGER(message => dbms_utility.format_error_stack||'; '||dbms_utility.format_error_backtrace, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out, is_error => 'error_file_date' );
            DEL_FILE( file_name );
        WHEN src_new_oper_id then
            L_LOGGER(message => dbms_utility.format_error_stack||'; '||dbms_utility.format_error_backtrace, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out, is_error => 'error_new_oper_id' );
        WHEN src_pass_oper_id then
            L_LOGGER(message => dbms_utility.format_error_stack||'; '||dbms_utility.format_error_backtrace, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out, is_error => 'error_pass_file' );
            DEL_FILE( file_name );
        WHEN OTHERS THEN 
            L_LOGGER(message => dbms_utility.format_error_stack||'; '||dbms_utility.format_error_backtrace, filename => file_name, id_dest => int_id_oper_dest, id_src => int_id_oper_src, only_dbms_output => only_dbms_out, is_error => 'error' );
            ROLLBACK;
            RAISE;
    END LOAD_FILE;


--===========================================================================================================================================================================
    PROCEDURE LOAD( server_directory in varchar2 default '/mnt/bs', database_directory in varchar2 default 'BS_SRC', only_dbms_out in number default null ) AS
        type vtab is table of varchar2(400);
        table_files	vtab;
        counter_files	pls_integer;
        file_name   varchar2(256);
        id_dest number;
        id_src number;
    
    BEGIN
            execute immediate 'alter session set parallel_force_local=true';
            execute immediate 'alter session set ddl_lock_timeout=3600';
            execute immediate 'alter session set sort_area_size=1395864371';
            execute immediate 'alter session set hash_area_size=1395864371';
            execute immediate 'alter session set workarea_size_policy=manual';
            execute immediate 'alter session enable parallel query';
            execute immediate 'alter session enable parallel dml';
            execute immediate 'alter session enable parallel ddl';
            
            get_dir_list_ext(server_directory);
            select t.filename bulk collect into table_files from dir_list_ext t where t.fd=0 order by t.filename asc;
            if table_files.count = 0 then
                return;
            end if;
    
            for counter_files in table_files.first..table_files.last loop
    
                BEGIN
                    file_name := table_files(counter_files);
                    LOAD_FILE(file_name, server_directory, database_directory, only_dbms_out );
    
                EXCEPTION 
                    WHEN OTHERS THEN 
                        RAISE;
                END;
    
            end loop;
    
    END LOAD;
--===========================================================================================================================================================================
    FUNCTION list_files return DIR_TYPE_SET   is
        res DIR_TYPE_SET ;
        pragma autonomous_transaction;
    
    BEGIN
        get_dir_list_ext('/mnt/bs');
        select   DIR_TYPE(fd, filename, fsize, fpath)
            bulk collect into res from dir_list_ext t where t.fd=0  order by t.filename asc;
        commit;
        return res;
    END;
--===========================================================================================================================================================================
END LOAD_GEO;
