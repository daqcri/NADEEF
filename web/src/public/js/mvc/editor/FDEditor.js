define([
    "text!mvc/editor/template/fd.template.html"
], function(Template) {

    function FDEditor(dom, table1, table2, rule) {
        this.dom = dom;
        this.table1 = table1;
        this.table2 = table2;
        this.rule = rule;
}

    FDEditor.prototype.render = function () {
        $(this.dom).html(_.template(Template, { code: this.rule.code }));
    };

    FDEditor.prototype.val = function() {
        return $(this.dom).find("#value").val();
    };

    return {
        Create: FDEditor
    }
});