/*****************************************************************
* SAS Contextual Analysis
* Concept Score Code
*
* Modify the following macro variables to match your needs.
* The liti_binary_path variable should have already been
* set to the location of the concept binary file for the
* associated SAS Contextual Analysis project.
****************************************************************/
/* Chequear si las variable fueron definidas en otro lugar, si esto esta usar par incrustar entre el codigo de SAS TEXT MINER  */
%sysfunc(ifc(%symexist(tm_defined_vars),, %nrstr(
/*La ruta del directorio contiene el data set, que le gustaría para el score */
    %let lib_path={put_your_directory_path_here};
    /* El data set que te gustaria para el score */
    %let input_ds = _my_lib.{put_your_data_set_name_here};
    /*La columna en el data set que contien el texto para el score */
    %let document_column = {put_your_document_column_name_here};
)));

libname _my_lib "&lib_path";
/* El boolean indicando si el documento columna es una referencia para la ruta (Y/N) */
%let is_reference = 'N';
/* El nombre del producto que el data set produce*/
%let output_position_ds = _out_pos_ds;

/* La ruta para el liti binario, deberia estar teniendo un set para su SCA procej´s ruta binaria del concepto*/

%let liti_binary_path = 'C:\\Users\\sasdemo\\Documents\\My SAS Files\\9.4\\dataton_stop\\config\\concepts.li';
/* El lenguaje.. deberia estar teniendo set para su lenguaje SCA project´*/
%let language = 'Spanish';

/* Asignar IDs para el documento*/
data _input_doc_ds;
    set &input_ds;
    _document_id = _N_;
run;

proc ds2 xcode=warning;
    require package tkcat; run;
    require package tktxtanio; run;
    require package tkling; run;

    thread work.t/overwrite = yes;

        /* Garantizar que solo el valor que se quiere se mantenga en la tabla*/
        keep _document_id
            _name
            _full_path
            _start_offset
            _end_offset
            _term
        ;

        dcl package tkcat cat();
        dcl package tktxtanio txtanio();
        dcl package tkling ling();

        dcl binary(8) _apply_settings;
        dcl binary(8) _case_mapping;
        dcl binary(8) _morphologic_dictionary;
        dcl binary(8) _liti_binary;
        dcl binary(8) _trans;
        dcl binary(8) _document;
        dcl double _status;
        dcl double _num_matches;
        dcl double _i;
        dcl double _document_id;
        dcl char(1024) _name;
        dcl char(1024) _full_path;
        dcl double _start_offset;
        dcl double _end_offset;
        dcl char(1024) _term;
        dcl char(200) _binfile;

        retain _apply_settings;
        retain _case_mapping;
        retain _morphologic_dictionary;
        retain _liti_binary;
        retain _trans;

        /* Sólo correr una vez cuando comienze */
        method init();
            _status = ling.set_language(&language);
            if _status NE 0 then put 'ERROR: set_language fails';

            _status = ling.set_default_language_data();
            if _status NE 0 then put 'ERROR: set_default_language_data fails';

            _apply_settings = cat.new_apply_settings();

            /*  Iniciar el caso mapeando el archivo */
            _binfile = ling.get_case_mapping_filename();
            _case_mapping = txtanio.new_local_file(_binfile);
            _status = cat.set_apply_case_mapping(_apply_settings, _case_mapping);
            if _status NE 0 then put 'ERROR: set_apply_case_mapping fails';

            _binfile = ling.get_mdic_filename();
            _morphologic_dictionary = txtanio.new_local_file(_binfile);
            _status = cat.set_apply_morphologic_dictionary(_apply_settings, _morphologic_dictionary);
            if _status NE 0 then put 'ERROR: set_apply_morphologic_dictionary fails';

            _liti_binary = txtanio.new_local_file(&liti_binary_path);
            _status = cat.set_apply_model(_apply_settings, _liti_binary);
            if _status NE 0 then put 'ERROR: set_apply_model fails';

            /* Hacer seguro que se tiene todo para el cruce */
            _status = cat.set_match_type(_apply_settings, 0);
            if _status NE 0 then put 'ERROR: set_match_type fails';

            _status = cat.initialize_concepts(_apply_settings);
            if _status NE 0 then put 'ERROR: initialize_concepts fails';

            _trans = cat.new_transaction();
        end;

        /* Correr por filas la tabla input*/
        method run();
            set _input_doc_ds(keep=(&document_column _document_id));

            /*  Iniciando el documento con la columna data */
            if &is_reference='N' then
                _document = txtanio.new_document_from_string(&document_column);
            else _document = txtanio.new_document_from_namedfile(&document_column);  ;

            /* Asegura que el documento sobre las transacciones, entonces, se tiene listo para el proceso */
            _status = cat.set_document(_trans, _document);
            if _status NE 0 then put 'ERROR: set_document fails on document:' _document_id;

            /* Aplicando para el documento binario*/
            _status = cat.apply_concepts(_apply_settings, _trans);
            if _status NE 0 then put 'ERROR: apply_concepts fails on document:' _document_id 'the observation variable is probably empty.';

            /* Mirar los conceptos para cruzar */
            _num_matches = cat.get_number_of_concepts(_trans);
            _i = 0;
            do while (_i LT _num_matches);
                _name = cat.get_concept_name(_trans, _i);
                _full_path = cat.get_full_path_from_name(_trans, _name);
                _start_offset = cat.get_concept_start_offset(_trans, _i);
                _end_offset = cat.get_concept_end_offset(_trans, _i);
                _term = cat.get_concept(_trans, _i);

                output;

                _i = _i + 1;
            end;

            /* Ahora, mirar los factores para cruzar*/
            _num_matches = cat.get_number_of_facts(_trans);
            _i = 0;
            do while (_i LT _num_matches);
                _name = cat.get_fact_name(_trans, _i);
                _full_path = cat.get_full_path_from_name(_trans, _name);
                _start_offset = cat.get_fact_start_offset(_trans, _i);
                _end_offset = cat.get_fact_end_offset(_trans, _i);
                _term = cat.get_fact(_trans, _i);

                output;

                _i = _i + 1;
            end;
            _i = 0;

            /* Limpiar los recursos */
            cat.clean_transaction(_trans);
            txtanio.free_object(_document);
        end;

        /* Sólo corre una vez terminé */
        method term();
            /* Limpiar los recursos */
            cat.free_transaction(_trans);
            txtanio.free_object(_liti_binary);
            txtanio.free_object(_morphologic_dictionary);
            txtanio.free_object(_case_mapping);
            cat.free_apply_settings(_apply_settings);
        end;
    endthread;
run;

/* Ahora crear la salida del data set */
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

    /* Correr por fila de del input */
    method run();
        set from t threads = &num_threads;
    end;
enddata;
run; quit;

/* Remover una columna asociada con los conceptos predefinidos desabilitados */
data &output_position_ds;
	set &output_position_ds
		(where=(name NOT IN ('')));
run;

proc sql noprint;
    drop table _input_doc_ds;
quit;