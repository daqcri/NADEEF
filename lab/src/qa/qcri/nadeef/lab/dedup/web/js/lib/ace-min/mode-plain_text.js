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

ace.define("ace/mode/plain_text",["require","exports","module","ace/lib/oop","ace/mode/text","ace/tokenizer","ace/mode/text_highlight_rules","ace/mode/behaviour"],function(e,t,n){var r=e("../lib/oop"),i=e("./text").Mode,s=e("../tokenizer").Tokenizer,o=e("./text_highlight_rules").TextHighlightRules,u=e("./behaviour").Behaviour,a=function(){this.HighlightRules=o,this.$behaviour=new u};r.inherits(a,i),function(){this.type="text",this.getNextLineIndent=function(e,t,n){return""},this.$id="ace/mode/plain_text"}.call(a.prototype),t.Mode=a})