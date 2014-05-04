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

    var op2int = {
        "EQ" : 0,
        "ED" : 1,
        "LS" : 2,
        "QG" : 3,
        "SD" : 4
    };

    var int2cmp = {
        0: '=',
        1: '>',
        2: '<',
        3: '>=',
        4: '<=',
        5: '!='
    };

    var cmp2int = {
        '=' : 0,
        '>' : 1,
        '<' : 2,
        '>=': 3,
        '<=': 4,
        '!=': 5
    };

    function parse(str) {
        var reg =
            new RegExp("(EQ|ED|LS|QG|SD)\\((\\w+)\\.(\\w+),(\\w+)\\.(\\w+)\\)(=|>|<|>=|<=|!=)(\\d+\\.?\\d*)");
        if (reg.test(str)) {
            var matches = reg.exec(str);
            return {
                op : matches[1],
                table1 : matches[2],
                column1 : matches[3],
                table2 : matches[4],
                column2 : matches[5],
                cmp : matches[6],
                val : matches[7]
            };
        }
        return null;
    }

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

        var conns = [];
        if (this.rule.code) {
            var ps = this.rule.code.split("\n");
            for (var i = 0; i < ps.length; i ++) {
                var tmp = parse(ps[i]);
                if (tmp.table1 === this.table1.name) {
                    tmp.from = this.table1;
                    tmp.to = this.table2;
                } else {
                    tmp.from = this.table2;
                    tmp.to = this.table1;
                }
                conns.push(tmp);
            }
        }

        $.when(promise1, promise2).then(function() {
            $(__.dom).html(_.template(ERTemplate) ({}));
            __.nodeEditor = new NodeEditor.Create(
                $(__.dom).find('#playground'),
                __.table1,
                __.table2,
                conns
            );

            // wrap up the right closure
            __.nodeEditor.onLineAdded(function(d) { __.onLineAdded(d); });
            __.nodeEditor.render();
        });
    };

    EREditor.prototype.onLineAdded = function(d) {
        this.update(this.nodeEditor.getConnection());
        var __ = this;
        var trs =
            d3.select("#ruleTableBody")
              .selectAll("tr")
              .data(this.predicates);

        trs.exit().remove();
        trs.enter().append("tr");
        trs.html(function (d, i) {
            return _.template(
                TableTemplate, {
                    id: i,
                    left: d.conn.left.getName(),
                    right: d.conn.right.getName(),
                    op: d.op,
                    cmp: d.cmp,
                    value: d.val
                });
        }).each(function(d, i) {
            $(__.dom).find("#del" + i).on("click", function () {
                console.log('del' + i);
                __.predicates.splice(i, 1);
                var conns = [];
                _.each(__.predicates, function (p) { conns.push(p.conn); });
                __.nodeEditor.updateConnection(conns);
                __.onLineAdded(d);
            });

            $(__.dom).find("#op" + i).change(function () {
                console.log('op' + i);
                __.predicates[i].op = $(__.dom).find("#op" + i).val();
            });

            $(__.dom).find("#cmp" + i).change(function () {
                console.log('cmp' + i);
                __.predicates[i].cmp = $(__.dom).find("#cmp" + i).val();
            });

            $(__.dom).find("#v" + i).change(function () {
                console.log('v' + i);
                __.predicates[i].val = $(__.dom).find("#v" + i).val();
            });
        });
    };

    EREditor.prototype.update = function(conns) {
        var toAdd = [];
        var __ = this;
        _.each(conns, function (x) {
            var found = _.find(__.predicates, function (p) { return x === p.conn; });
            if (_.isUndefined(found))
                toAdd.push(x);
        });

        _.each(toAdd, function (x) {
            __.predicates.push({ conn : x, op : 0, cmp : 0, val : 1 });
        });
    };

    EREditor.prototype.val = function() {
        if (_.isEmpty(this.predicates))
            return null;

        var cmd = "";
        _.each(this.predicates, function(d) {
            cmd += int2op[d.op];
            cmd += "(";
            cmd += d.conn.left.getName();
            cmd += ",";
            cmd += d.conn.right.getName();
            cmd += ")";
            cmd += int2cmp[d.cmp];
            cmd += d.val;
            cmd += "\n";
        });

        return cmd;
    };

    return {
        Create: EREditor
    };
});