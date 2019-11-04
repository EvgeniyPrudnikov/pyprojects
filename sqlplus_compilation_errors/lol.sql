set echo on;
set timing on;
set serveroutput on;

create or replace procedure my_poc1
as
begin
 NULL;
end;
/
show errors;
PAUSE check for errors

create or replace procedure my_poc2
as
begin
 NULL
end;
/
show errors;
PAUSE check for errors

create or replace procedure my_poc3
as
begin
 NULL;
end;
/
show errors;
PAUSE check for errors

exit;