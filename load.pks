CREATE OR REPLACE PACKAGE LOAD_GEO AS   
    
    file_date_less_then_act EXCEPTION;
    PRAGMA EXCEPTION_INIT (file_date_less_then_act,-20100);
    src_new_oper_id EXCEPTION;
    PRAGMA EXCEPTION_INIT (src_new_oper_id,-20101);
    src_pass_oper_id EXCEPTION;
    PRAGMA EXCEPTION_INIT (src_pass_oper_id,-20102);  

    PROCEDURE LOAD(server_directory in varchar2 default '/mnt/bs', database_directory in varchar2 default 'BS_SRC', only_dbms_out in number default null);
    PROCEDURE LOAD_FILE(file_name in varchar2, server_directory in varchar2 default '/mnt/bs', database_directory in varchar2 default 'BS_SRC', only_dbms_out in number default null);

    FUNCTION LIST_FILES return DIR_TYPE_SET;
END LOAD_GEO;
