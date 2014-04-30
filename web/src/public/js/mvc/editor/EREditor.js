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
    "mvc/editor/node-editor",
    "requester",
    "text!mvc/editor/template/er.template.html"
], function(NodeEditor, Requester, ERTemplate) {
    var table1 = null;
    var table2 = null;

    function render(id, tableName1, tableName2) {
        var promise1 = null;
        if (_.isEmpty(tableName1)) {
            promise1 = null;
            table1 = null;
        } else
            promise1 = Requester.getTableSchema(tableName1, {
                success: function(data) {
                    table1 = { name : tableName1.substr(3), columns: data['schema']};
                }
            });

        var promise2 = null;
        if (_.isEmpty(tableName2)) {
            promise2 = null;
            tableName2 = null;
        } else
            promise2 = Requester.getTableSchema(tableName2, {
                success: function(data) {
                    table2 = { name : tableName2.substr(3), columns: data['schema']};
                }
            });

        $.when(promise1, promise2).then(function() {
            $(id).html(_.template(ERTemplate) ({}));
            NodeEditor.start(table1, table2);
        });
    }

    function int2op(x) {
        var op;
        switch(x) {
            default:
            case 0:
                op = 'EQ'; break;
            case 1:
                op = 'ED'; break;
            case 2:
                op = 'LS'; break;
            case 3:
                op = 'QG'; break;
            case 4:
                op = 'SD'; break;
        }
        return op;
    }

    function int2cmp(x) {
        var cop;
        switch(x) {
            default:
            case 0:
                cop = '='; break;
            case 1:
                cop = '>'; break;
            case 2:
                cop = '<'; break;
            case 3:
                cop = '>='; break;
            case 4:
                cop = '<='; break;
            case 5:
                cop = '!='; break;
        }
        return cop;
    }

    function val() {
        var conns = NodeEditor.getRule();

        if (_.isNull(conns) || _.isEmpty(conns) || conns.length == 0)
            return null;

        var cmd = "";
        _.each(conns, function(d) {
            cmd += int2op(d[2]);
            cmd += "(";
            cmd += table1.name + "." + table1.columns[d[0].substr(1)];
            cmd += ",";
            cmd += table2.name + "." + table2.columns[d[1].substr(1)];
            cmd += ")";
            cmd += int2cmp(parseInt(d[3]));
            cmd += d[4];
            cmd += "\n";
        });

        return cmd;
    }

    return {
        render : render,
        val : val
    };
});