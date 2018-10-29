/*****************************************************************
 * SAS Contextual Analysis
 * Categories Score Code
 *
 * Modify the following macro variables to match your needs.
 * The "mco_binary_path" variable should have already been
 * set to point at the categories binary for the
 * associated SAS Contextual Analysis project.
 ****************************************************************/
/* Chequear si las variables que son definidas en este lugar, esto es usado para incrustar el codigo dentro de SAS TEXT MINER*/
%sysfunc(ifc(%symexist(tm_defined_vars),, %nrstr(
/* La ruta del directorio contiene el data set que se tiene para el score*/
    %let lib_path={put_your_directory_path_here};
	/* El data set que le gustaria para el score*/
    %let input_ds = _my_lib.{put_your_data_set_name_here};
	/* La columna en el data set que contiene el text data para el score*/
    %let document_column = {put_your_document_column_name_here};
)));

libname _my_lib "&lib_path";
/* El boolean indicando si el documento columna es una referencia del tipo path (Y/N)*/
%let is_reference = 'N';
/* El nombre de la salida que produce el data set */
%let output_position_ds = _out_pos_ds;
/* El nombre de la salida del documento que produce el data set*/
%let output_document_ds = _out_docs_ds;
/*La ruta para el mco binario, deberia estar teniendo el set de su SCA project's de la ruta de categoria binanria*/

%let mco_binary_path = 'C:\\Users\\sasdemo\\Documents\\My SAS Files\\9.4\\dataton_stop\\config\\categories.mco';

/* Asignacios de IDs para los documentos */
data _input_doc_ds;
    set &input_ds;
    _document_id = _N_;
run;

proc ds2 xcode=warning;
    require package tkcat; run;
    require package tktxtanio; run;

    thread work.t/overwrite = yes;

        /* Asegurar que solo los valores que se quieren en la salida sean mantenidos */
        keep _document_id
             _name
             _full_path
             _start_offset
             _end_offset
             _term
        ;

        dcl package tkcat cat();
        dcl package tktxtanio txtanio();

        dcl double _document_id;
        dcl double _start_offset;
        dcl double _end_offset;
        dcl char(1024) _name;
        dcl char(1024) _full_path;
        dcl char(1024) _term;
        dcl binary(8) _apply_settings;
        dcl binary(8) _mco_binary;
        dcl binary(8) _trans;
        dcl binary(8) _document;
        dcl double _status;
        dcl double _num_matches;
        dcl double _i;
        dcl double _num_terms;
        dcl double _j;

        retain _apply_settings;
        retain _mco_binary;
        retain _trans;

        /* Solo corre una ves que comienza */
        method init();
            _apply_settings = cat.new_apply_settings();

            _mco_binary = txtanio.new_local_file(&mco_binary_path);
            cat.set_categories_model(_apply_settings, _mco_binary);
            cat.set_return_match_positions_for_categories(_apply_settings, 1);
            cat.set_relevancy_type(_apply_settings, 1);

            _status = cat.initialize_categories(_apply_settings);
            if _status NE 0 then put 'ERROR: initialize_categories fails';

            _trans = cat.new_transaction();
        end;

        /* Correr por fila los inputs */
        method run();
            set _input_doc_ds(keep=(&document_column _document_id));

            /* Iniciando el documento con la columna de datos */
            if &is_reference='N' then
                _document = txtanio.new_document_from_string(&document_column);
            else _document = txtanio.new_document_from_namedfile(&document_column);  ;

            /* El set de el documento sobre la transacción, entonces estamos listo para el proceso */
            _status = cat.set_document(_trans, _document);
            if _status NE 0 then put 'ERROR: set_document fails on document:' _document_id;

            /* Aplicando el binario para el documento */
            _status = cat.apply_categories(_apply_settings, _trans);
            if _status NE 0 then put 'ERROR: apply_categories fails on document:' _document_id 'the observation variable is probably empty.';

            /* Tener el numero de categorios para cruzar */
            _num_matches = cat.get_nb_matched_categories(_trans);

            _i = 0;
            do while (_i LT _num_matches);
			/*Se usa el name para ambos y la ruta completa para una mejor alineacion con el código LITI */
                _name = cat.get_category_name(_trans, _i);
                _full_path = _name;

                _num_terms = cat.get_nb_matched_terms_category(_trans, _i);

                _j = 0;
                do while (_j LT _num_terms);
                    _start_offset = cat.get_matched_term_start_offset_category(_trans, _i, _j);
                    _end_offset = cat.get_matched_term_end_offset_category(_trans, _i, _j);
                    _term = cat.get_matched_term_category(_trans, _i, _j);

                    output;

                    _j = _j + 1;
                end;

                _i = _i + 1;
            end;

            /* limpiar los recursos */
            cat.clean_transaction(_trans);
            txtanio.free_object(_document);
        end;

        /* Sólo se corre una vez termine  */
        method term();
            /* Limpiar los recursos*/
            cat.free_transaction(_trans);
            cat.free_apply_settings(_apply_settings);
            txtanio.free_object(_mco_binary);
        end;
    endthread;
run;

/* Ahora, crear la salida del data set */
%let num_threads=%sysfunc(getoption(CPUCOUNT));

data &output_position_ds(
    rename = (_document_id=document_id
            _name=name
            _full_path=full_path
            _start_offset=start_offset
            _end_offset=end_offset
            _term=term
           )
    ) / overwrite = yes ;
    dcl thread work.t t;

    /* Correr por fila los input */
    method run();
        set from t threads = &num_threads;
    end;
enddata;
run; quit;

data _tmp_position_ds(drop=num_tokens i last_pos);
    retain last_pos;
    set &output_position_ds(drop=name);

    if _N_ = 1 then do;
        name = full_path;
    end;

    num_tokens = countc(full_path, '/', 'o');
    last_pos = findc(full_path, '/', 'o');
    do i = 2 to num_tokens + 1;
        last_pos = findc(full_path, '/', 'o', last_pos + 1);
        if last_pos gt 0 then do;
            name = substr(full_path, 1, last_pos - 1);
        end;
        else do;
            name = full_path;
        end;
        output;
    end;
run;

/* Tener unicamente la categoría nombre y documento */
proc sort data=_tmp_position_ds out=_tmp_doc_cat_ds(rename=(document_id = _document_)) nodupkey;
    by name document_id;
run;

/* Asignar un nombre a la columna por categoría */
data _tmp_doc_cat_ds (keep = name _document_ column_name in_category_rule);
    length column_name $8.;

    set _tmp_doc_cat_ds;
    by name _document_;

    retain previous_name column_name index;

    if _n_ = 1 then do;
        previous_name = name;
        column_name = "c_1";
        index = 1;
    end;

    if name NE previous_name then do;
        index = index + 1;
        previous_name = name;
        column_name = "c_" || kleft(index);
    end;

    in_category_rule = 1;
run;

/* Clasificar sobre el ID documento  */
proc sort data=_tmp_doc_cat_ds;
    by _document_ name;
run;

/* Transponer, entonces que se tiene una columna por cada categoria */
proc transpose data = _tmp_doc_cat_ds out = _tmp_transposed_ds(drop=_NAME_);
    var in_category_rule;
    by _document_;
    id column_name;
    idlabel name;
run;

/* Cruzar la categoria de las columnas con el data set de salida */
data _tmp_missing_ds;
    merge _tmp_transposed_ds _input_doc_ds (rename=(_document_id=_document_));
    by _document_;
run;

/* convertir missing values a ceros */
data &output_document_ds;
    set _tmp_missing_ds;
    array ss[*] c_ : ;
    do j = 1 to dim(ss);
    if ss(j) = . then ss(j) = 0;
    end;
    drop j;
run;

/* Ordenar la posición del data set */
proc sort data=&output_position_ds;
    by document_id name;
run;

/* Mapeo del nombre del las categorias en la posición del data set */
data &output_position_ds;
    merge &output_position_ds _tmp_doc_cat_ds(rename=(_document_=document_id) drop=in_category_rule);
    by document_id name;
run;

/* Eliminar los data sets intermedios */
proc sql noprint;
    drop table _tmp_position_ds;
    drop table _tmp_doc_cat_ds;
    drop table _tmp_transposed_ds;
    drop table _tmp_missing_ds;
    drop table _input_doc_ds;
quit;