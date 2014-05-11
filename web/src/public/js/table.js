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

define(["requester", "state"], function(Requester, State) {
    var instance = null;
    var cache = {};

    function load(table, reload) {
        if (_.isNull(table) || _.isNaN(table)) {
            console.log("Table name is null or empty.");
            return;
        }

        reload = _.isUndefined(reload) ? true : reload;
        var domId = table.domId;
        var tablename = table.table;

        if (cache[domId] != null && !reload) {
            if (domId != 'source-table' ||
                (domId == 'source-table' && cache[domId].source == tablename)) {
                console.log('cache hit on table ' + domId);
                return;
            }

        }

        if (cache[domId] != null) {
            try {
                cache[domId].instance.fnDestroy();
            } catch (e) {
                console.log(e);
            }
        }

        if (domId == 'source-table') {
            $("#source-table-info").addClass('hide');
            $('#source-table').removeClass('hide');
        }

        Requester.getTableSchema(
            tablename,
            { success: function(data) {
                var schema = data['schema'];
                var columns = [];
                $('#' + domId).empty().append('<thead><tr></tr></thead>');
                for (var i = 0; i < schema.length; i ++) {
                    columns.push( { sTitle : schema[i] } );
                }

                var project = State.get('project');
                var url = "/" + project + "/table/" + tablename;
                /* Add a select menu for each TH element in the table footer */
                instance = $('#' + domId).DataTable({
                    "scrollX" : true,
                    "bAutoWidth" : true,
                    "bSort": false,
                    "bProcessing" : true,
                    "bDestroy": true,
                    "bServerSide": true,
                    "sAjaxSource": url,
                    "sAjaxDataProp": 'data',
                    "aoColumns" : columns
                });

                cache[domId] = { instance : instance, source : tablename };
            }}
        );
    }

    function filter(e) {
        if (instance != null) {
            instance.fnFilter(e);
        }
    }

    return {
        load : load,
        filter : filter
    };
});

