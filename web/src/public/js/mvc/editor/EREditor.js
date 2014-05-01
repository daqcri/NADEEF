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
    "mvc/editor/NodeEditor",
    "requester",
    "text!mvc/editor/template/er.template.html",
    "text!mvc/editor/template/property.template.html"
], function(NodeEditor, Requester, ERTemplate, TableTemplate) {
    var int2op = {
        0: 'EQ',
        1: 'ED',
        2: 'LS',
        3: 'QG',
        4: 'SD'
    };

    var int2cmp = {
        0: '=',
        1: '>',
        2: '<',
        3: '>=',
        4: '<=',
        5: '!='
    };

    function EREditor(dom, table1, table2, rule) {
        this.dom = dom;
        this.tableName1 = table1;
        this.tableName2 = table2;
        this.rule = rule;
        this.editor = null;
        this.predicates = [];
    }

    EREditor.prototype.render = function () {
        var promise1 = null;
        var __ = this;
        if (_.isEmpty(this.tableName1)) {
            promise1 = null;
            this.tableName1 = null;
        } else {
            promise1 = Requester.getTableSchema(this.tableName1, {
                success: function (data) {
                    __.table1 = { name: __.tableName1.substr(3), columns: data['schema']};
                }
            });
        }

        var promise2 = null;
        if (_.isEmpty(this.tableName2)) {
            promise2 = null;
            this.tableName2 = null;
        } else {
            promise2 = Requester.getTableSchema(this.tableName2, {
                success: function (data) {
                    __.table2 = { name: __.tableName2.substr(3), columns: data['schema']};
                }
            });
        }

        $.when(promise1, promise2).then(function() {
            $(__.dom).html(_.template(ERTemplate) ({}));
            __.nodeEditor = new NodeEditor.Create(
                $(__.dom).find('#playground'),
                __.table1,
                __.table2
            );

            __.nodeEditor.onLineAdded(__.onLineAdded);
            __.nodeEditor.render();
        });
    };

    EREditor.prototype.onLineAdded = function() {
        this.update(this.nodeEditor.getConnection());
        var __ = this;

        var trs =
            $(this.dom).find("#ruleTableBody")
                .selectAll("tr")
                .data(this.predicates);

        trs.exit().remove();
        trs.enter().append("tr");
        trs.html(function (d, i) {
            return _.template(
                TableTemplate, {
                    id: i,
                    left: d.conn.left.getName(),
                    right: d.right.getName(),
                    op: d.op,
                    cmp: d.cmp,
                    value: d.val
                });
        }).each(function(d, i) {
            __.dom.find("#del" + i).on("click", function () {
                __.predicates.splice(i, 1);
                var conns = [];
                _.each(conns, function (conn) { __.predicates.push(conn); });
                __.nodeEditor.updateConnection(conns);
            });

            __.dom.find("#op" + i).change(function () {
                __.predicates[i].op = __.dom.find("#op" + i + " option:selected").val();
            });

            __.dom.find("#cmp" + i).change(function () {
                __.predicates[i].cmp = __.dom.find("#cmp" + i + " option:selected").val();
            });

            __.dom.find("#v" + i).change(function () {
                __.predicates[i].val = __.dom.find("#v" + i + " option:selected").val();
            });
        });
    };

    EREditor.prototype.update = function(conns) {
        var toAdd = [];
        _.each(conns, function (x) {
            var found = _.find(this.predicates, function (p) { return x === p.conn; });
            if (_.isUndefined(found))
                toAdd.push(x);
        });

        _.each(toAdd, function (x) {
            this.predicates.push({ conn : x, op : 0, cmp : 0, val : 1 });
        });
    };

    EREditor.prototype.val = function() {
        var conns = this.nodeEditor.getConnection();
        if (_.isEmpty(conns))
            return null;

        this.update(conns);
        var cmd = "";
        var __ = this;
        _.each(conns, function(d) {
            cmd += int2op[d.op];
            cmd += "(";
            cmd += d.conn.table1.name + "." + __.table1.columns[d[0].substr(1)];
            cmd += ",";
            cmd += __.table2.name + "." + __.table2.columns[d[1].substr(1)];
            cmd += ")";
            cmd += int2cmp[parseInt(d[3])];
            cmd += d[4];
            cmd += "\n";
        });

        return cmd;
    };

    return {
        Create: EREditor
    };
});