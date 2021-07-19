--====================================================================================================
-- create output format for java procedure
CREATE OR REPLACE TYPE      "DIR_TYPE" as object (
  fd   number,
  fname   varchar2(256),
  fsize   number,
  fpath   varchar2(4095)
);
create or replace TYPE      "DIR_TYPE_SET" as table of dir_type;
--==================================================================================================== 
 
CREATE TABLE BS_SRC_LOG (
  ID NUMBER 
, DATE_LOG DATE 
, FILENAME VARCHAR2(500 BYTE) 
, ID_DEST NUMBER 
, ID_SRC NUMBER 
, IS_ERROR VARCHAR2(500 BYTE) 
, MESSAGE VARCHAR2(4000 BYTE) 
, DATE_START DATE 
); 
--====================================================================================================
-- Partitioned table
CREATE TABLE BS_FINAL (
ID_SRC
...
, LAC NUMBER NOT NULL 
, CELL NUMBER NOT NULL 
...
, latitude NUMBER NOT NULL 
, longitude NUMBER NOT NULL 

, SH1 RAW(40) 
) 
TABLESPACE BS_NEW 
PCTFREE 0 
COMPRESS 
PARALLEL 8 
PARTITION BY LIST (ID_SRC) (
  PARTITION BS_1 VALUES (1) 
  TABLESPACE BS_NEW 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    INITIAL 8388608 
    NEXT 1048576 
    MINEXTENTS 1 
    MAXEXTENTS UNLIMITED 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS
, PARTITION BS_2 VALUES (2) 
  TABLESPACE BS_NEW 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    INITIAL 8388608 
    NEXT 1048576 
    MINEXTENTS 1 
    MAXEXTENTS UNLIMITED 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS
, 
...
...
... 
, PARTITION OTHERS VALUES (DEFAULT) 
  TABLESPACE BS_NEW 
  PCTFREE 0 
  INITRANS 1 
  STORAGE 
  ( 
    INITIAL 8388608 
    NEXT 1048576 
    MINEXTENTS 1 
    MAXEXTENTS UNLIMITED 
    BUFFER_POOL DEFAULT 
  ) 
  COMPRESS 
);

-- Indexes
CREATE INDEX BS_PULT_DOLGOTA_INDEX ON BS_PULT (TRUNC("DOLGOTA",3) ASC, SHIROTA ASC) 
LOGGING 
TABLESPACE BS_INDEX 
PCTFREE 10 
INITRANS 2 
STORAGE 
( 
  INITIAL 65536 
  NEXT 1048576 
  MINEXTENTS 1 
  MAXEXTENTS UNLIMITED 
  BUFFER_POOL DEFAULT 
) 
PARALLEL 8;

CREATE UNIQUE INDEX BS_PULT_MAIN_IINDEX ON BS_PULT (CELL ASC, LAC ASC, ID_SRC ASC, "END_DATE" DESC) 
LOGGING 
TABLESPACE BS_INDEX 
PCTFREE 10 
INITRANS 2 
STORAGE 
( 
  INITIAL 65536 
  NEXT 1048576 
  MINEXTENTS 1 
  MAXEXTENTS UNLIMITED 
  BUFFER_POOL DEFAULT 
) 
PARALLEL 8;
--====================================================================================================