/*****************************************************************
* SAS Contextual Analysis
* Concept Score Code
*
* Modify the following macro variables to match your needs.
* The liti_binary_path variable should have already been
* set to the location of the concept binary file for the
* associated SAS Contextual Analysis project.
****************************************************************/
/* check if the variables were defined elsewhere - this is used for embedding code into SAS Text Miner */
%sysfunc(ifc(%symexist(tm_defined_vars),, %nrstr(
    /* the path to the directory containing the data set you would like to score */
    %let lib_path={put_your_directory_path_here};
    /* the data set you would like to score */
    %let input_ds = _my_lib.{put_your_data_set_name_here};
    /* the column in the data set that contains the text data to score */
    %let document_column = {put_your_document_column_name_here};
)));

libname _my_lib "&lib_path";
/* boolean indicating if the document column is a reference to a file path (Y/N) */
%let is_reference = 'N';
/* the name of the output data set to produce */
%let output_position_ds = _out_pos_ds;
/* the path to the liti binary... should have been set to your SCA project's concept binary path */
%let liti_binary_path = 'C:\\Users\\sasdemo\\Documents\\My SAS Files\\9.4\\dataton_stop\\config\\concepts.li';
/* the language... should have been set to your SCA project's language */
%let language = 'Spanish';

/* assign IDs to the documents */
data _input_doc_ds;
    set &input_ds;
    _document_id = _N_;
run;

proc ds2 xcode=warning;
    require package tkcat; run;
    require package tktxtanio; run;
    require package tkling; run;

    thread work.t/overwrite = yes;

        /* ensure that only the values we want in the output table are kept */
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

        /* only runs once when starting */
        method init();
            _status = ling.set_language(&language);
            if _status NE 0 then put 'ERROR: set_language fails';

            _status = ling.set_default_language_data();
            if _status NE 0 then put 'ERROR: set_default_language_data fails';

            _apply_settings = cat.new_apply_settings();

            /* intialize the case mapping file */
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

            /* make sure we get ALL MATCHES */
            _status = cat.set_match_type(_apply_settings, 0);
            if _status NE 0 then put 'ERROR: set_match_type fails';

            _status = cat.initialize_concepts(_apply_settings);
            if _status NE 0 then put 'ERROR: initialize_concepts fails';

            _trans = cat.new_transaction();
        end;

        /* run per row of input */
        method run();
            set _input_doc_ds(keep=(&document_column _document_id));

            /* initialize the document with the column data */
            if &is_reference='N' then
                _document = txtanio.new_document_from_string(&document_column);
            else _document = txtanio.new_document_from_namedfile(&document_column);  ;

            /* set the document on the transaction so we're ready to process */
            _status = cat.set_document(_trans, _document);
            if _status NE 0 then put 'ERROR: set_document fails on document:' _document_id;

            /* apply the binary to the document */
            _status = cat.apply_concepts(_apply_settings, _trans);
            if _status NE 0 then put 'ERROR: apply_concepts fails on document:' _document_id 'the observation variable is probably empty.';

            /* look for the concept matches */
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

            /* now look for fact matches */
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

            /* clean up resources */
            cat.clean_transaction(_trans);
            txtanio.free_object(_document);
        end;

        /* only runs once at end */
        method term();
            /* clean up resources */
            cat.free_transaction(_trans);
            txtanio.free_object(_liti_binary);
            txtanio.free_object(_morphologic_dictionary);
            txtanio.free_object(_case_mapping);
            cat.free_apply_settings(_apply_settings);
        end;
    endthread;
run;

/* now create output data set */
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

    /* run per row of input */
    method run();
        set from t threads = &num_threads;
    end;
enddata;
run; quit;

/* remove any columns associated with disabled predefined concepts */
data &output_position_ds;
	set &output_position_ds
		(where=(name NOT IN ('')));
run;

proc sql noprint;
    drop table _input_doc_ds;
quit;