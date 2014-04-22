/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

define([
    'state',
    'text!mvc/template/sourceeditor.template.html'
], function(State, SourceEditorTemplate) {
    function start(id) {
        var html =
            _.template(SourceEditorTemplate)();
        $('#' + id).html(html);

        // initialize the filedrop
        $("#dropbox").filedrop({
            maxfiles: 5,
            maxfilesize: 500,
            queuefiles: 1,
            url: "/do/upload",
            allowedfileextensions: ['.csv'],
            data: { project: State.get("project") },
            error: function(err, file) {
                switch(err) {
                    case 'BrowserNotSupported':
                        alert('Your browser does not support HTML5 file uploads!');
                        break;
                    case 'TooManyFiles':
                        alert('Too many files! Please select 5 at most!');
                        break;
                    case 'FileTooLarge':
                        alert(file.name + ' is too large! Please upload files up to 500 mb.');
                        break;
                    default:
                        break;
                }
            },

            uploadStarted: function(i, file, len) {
                // a file began uploading
                // i = index => 0, 1, 2, 3, 4 etc
                // file is the actual file of the index
                // len = total files user dropped
                $("#dropbox-message").text('Uploading ' + file.name);
                $("#source-editor-save").prop('disabled', true);
                $("#source-editor-cancel").prop('disabled', true);
            },

            uploadFinished: function(i, file, response, time) {
                // response is the data you got back from server in JSON format.
                // alert("hello");
                // alert(time);
                $("#dropbox-message").text('Drop CSV file here to upload.');
                $("#source-editor-save").prop('disabled', false);
                $("#source-editor-cancel").prop('disabled', false);
                $('#dropbox-progress div').width(0);
            },

            progressUpdated: function(i, file, progress) {
                // this function is used for large files and updates intermittently
                // progress is the integer value of file being uploaded percentage to completion
            },

            globalProgressUpdated: function(progress) {
                // progress for all the files uploaded on the current instance (percentage)
                // ex: $('#progress div').width(progress+"%");
                $('#dropbox-progress div').width(progress + "%");
            }
        });
    }

    return {
        start: start
    };
});
