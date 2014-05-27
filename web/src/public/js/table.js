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
    var instance1 = null;
    var instance2 = null;

    function load(table) {
        if (_.isNull(table) || _.isNaN(table)) {
            console.log("Table name is null or empty.");
            return;
        }

        var domId = table.domId;
        var tablename = table.table;
        var rule = table.rule;

        if ("source-table" === domId) {
            $("#source-table-info").addClass('hide');
            $('#source-table').removeClass('hide');
        }

        if (domId == 'violation-table') {
            $("#violation-table-info").addClass('hide');
            $('#violation-table-div').removeClass('hide');
        }

        if (tablename === "violation") {
            drawViolationTable(domId, rule);
        } else {
            Requester.getTableSchema(
                tablename, {
                    success: function (data) {
                        var schema = data['schema'];
                        var columns = [];
                        var dom = $('#' + domId);
                        if ($.fn.dataTable.isDataTable('#' + domId))
                            dom.DataTable().destroy();
                        dom.empty().append('<thead><tr></tr></thead>');
                        for (var i = 0; i < schema.length; i++)
                            columns.push({ sTitle: schema[i] });

                        var project = State.get('project');
                        var url = "/" + project + "/table/" + tablename;
                        instance1 = dom.DataTable({
                            "scrollX": true,
                            "ordering": false,
                            "processing": true,
                            "serverSide": true,
                            "ajax": { "url": url, "dataSrc": 'data' },
                            "destroy": true,
                            "columns": columns,
                            "ajaxDataProp": 'data'
                        });
                    }}
            );
        }
    }

    function createViolationTable(domId, tableName, rule, isFirst, length) {
        var schema; // evil closure
        Requester.getTableSchema(tableName, {
            success: function (data) {
                var dom = $('#' + domId);
                var columns = [];
                var instance;
                var project = State.get('project');
                var ruleQuery = "";
                schema = data['schema'];
                columns = [];

                if ($.fn.dataTable.isDataTable('#' + domId))
                    dom.DataTable().destroy();
                dom.empty().append('<thead><tr></tr></thead>');
                for (var i = 0; i < schema.length; i++)
                    columns.push({ sTitle: schema[i] });

                for (var i = 0; i < rule.length; i++)
                    ruleQuery += "rule=" + rule[i];

                var url = "/" + project + "/violation/" + tableName + "?" + ruleQuery;
                instance = dom.DataTable({
                    "createdRow": function (row, data) {
                        var vid = data[data.length - 2];
                        var attrToken = data[data.length - 1];
                        var rainbow = ["#dff0d8", "#fcf8e3", "#d9edf7", "#f2dede"];
                        // custom parsing
                        var cells = $('td', row);
                        cells.attr('data', vid);
                        for (var i = 0, pre = 0; i < attrToken.length; i++) {
                            if ('{' === attrToken[i] ||
                                '}' === attrToken[i] ||
                                ',' === attrToken[i]) {
                                var attr = attrToken.substring(pre, i);
                                if (!_.isEmpty(attr)) {
                                    var cell = cells.eq(_.indexOf(schema, attr));
                                    cell.css("background-color", rainbow[vid % rainbow.length])
                                        .attr("data-toggle", "tooltip")
                                        .attr("font-weight", "bolder")
                                        .attr("title", "vid: " + vid);
                                }
                                pre = i + 1;
                            }
                        }

                        // create event for click filter
                        cells.on("click", function(e) {
                            var vid = this.getAttribute("data");
                            if (instance1 != null)
                                instance1.search(":=" + vid).draw();
                            if (instance2 != null)
                                instance2.search(":=" + vid).draw();
                        });

                        // truncate large text

                        for (var i = 0; i < data.length; i ++) {
                            if (!_.isEmpty(data[i]) && data[i].length > 300) {
                                var label = '... <span class="label label-info" ' +
                                    'data-toggle="tooltip" ' +
                                    'title="' + data[i] + '">More</span>';
                                cells.eq(i).html(data[i].substring(0, 300) + label);
                            }
                        }
                    },
                    "initComplete": function() {
                        $(document).off('keyup').on('keyup', function (e) {
                            if (e.which == 27) {
                                if (instance1 != null)
                                    instance1.search("").draw();
                                if (instance2 != null)
                                    instance2.search("").draw();
                            }
                        });
                    },
                    "scrollX": true,
                    "ordering": false,
                    "processing": true,
                    "serverSide": true,
                    "ajax": { "url": url, "dataSrc": 'data' },
                    "destroy": true,
                    "columns": columns,
                    "ajaxDataProp": 'data',
                    "lengthMenu": _.isUndefined(length) ? [10, 25, 50] : length
                });

                if (isFirst)
                    instance1 = instance;
                else
                    instance2 = instance;
            }
        });
    }

    // render of violation table
    function drawViolationTable(domId, rule) {
        Requester.getViolationMetaData(rule, {
            success: function (data) {
                var tables =
                    data.data.length < 1 ?
                        [[0, State.get("currentSource")]] : data.data;
                if (tables.length > 1) {
                    instance1 = createViolationTable(
                        domId, tables[0][1], rule, true, [3, 10, 25, 50]);
                    instance2 = createViolationTable(
                        "violation-table-extra",
                        tables[1][1],
                        rule,
                        false,
                        [3, 10, 25, 50]
                    );
                    $("#violation-table-extra").removeClass("hide");
                } else {
                    $("#violation-table-extra").addClass("hide");
                    createViolationTable(domId, tables[0][1], rule);
                }
            }
        });
    }

    function filter(e) {
        if (instance1 != null)
            instance1.search(e).draw();
        if (instance2 != null)
            instance2.search(e).draw();
    }

    return {
        load : load,
        filter : filter
    };
});

