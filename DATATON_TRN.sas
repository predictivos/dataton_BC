/* Con este codigo se realiza el proceso de transformación de los datos de csv a tablas SAS, esto con el fin
de poder realizar el análisis*/

libname SCA BASE "D:\data\sca";

FILENAME CSV "D:\data\Dataton\ref1.csv" TERMSTR=LF ;*encoding=UTF-8;

data SCA.REF    ;
  %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
  infile CSV delimiter = ',' MISSOVER DSD  firstobs=1;
    informat REF1 $50. ;
    format REF1 $50. ;
     input
                 REF1  $
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;
     
/** Unassign the file reference.  **/

FILENAME CSV;


libname inlib cvp 'D:\data\Dataton';
libname outlib 'D:\data\DATATONUTF8' outencoding='UTF-8'; 

proc copy noclone in=inlib out=outlib; 
select DATATON_COMP; 
run;



