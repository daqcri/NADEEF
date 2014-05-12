define([
    "text!mvc/editor/template/dc.template.html"
], function(Template) {

    function DCEditor(dom, table1, table2, rule) {
        this.dom = dom;
        this.table1 = table1;
        this.table2 = table2;
        this.rule = rule;
    }

    DCEditor.prototype.render = function () {
        $(this.dom).html(_.template(Template, { code : this.rule.code }));
    };

    DCEditor.prototype.val = function() {
        return $(this.dom).find("#value").val();
    };

    return {
        Create: DCEditor
    }
});