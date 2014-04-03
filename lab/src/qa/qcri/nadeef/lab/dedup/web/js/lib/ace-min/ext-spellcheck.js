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

ace.define("ace/ext/spellcheck",["require","exports","module","ace/lib/event","ace/editor","ace/config"],function(e,t,n){var r=e("../lib/event");t.contextMenuHandler=function(e){var t=e.target,n=t.textInput.getElement();if(!t.selection.isEmpty())return;var i=t.getCursorPosition(),s=t.session.getWordRange(i.row,i.column),o=t.session.getTextRange(s);t.session.tokenRe.lastIndex=0;if(!t.session.tokenRe.test(o))return;var u="\x01\x01",a=o+" "+u;n.value=a,n.setSelectionRange(o.length,o.length+1),n.setSelectionRange(0,0),n.setSelectionRange(0,o.length);var f=!1;r.addListener(n,"keydown",function l(){r.removeListener(n,"keydown",l),f=!0}),t.textInput.setInputHandler(function(e){console.log(e,a,n.selectionStart,n.selectionEnd);if(e==a)return"";if(e.lastIndexOf(a,0)===0)return e.slice(a.length);if(e.substr(n.selectionEnd)==a)return e.slice(0,-a.length);if(e.slice(-2)==u){var r=e.slice(0,-2);if(r.slice(-1)==" ")return f?r.substring(0,n.selectionEnd):(r=r.slice(0,-1),t.session.replace(s,r),"")}return e})};var i=e("../editor").Editor;e("../config").defineOptions(i.prototype,"editor",{spellcheck:{set:function(e){var n=this.textInput.getElement();n.spellcheck=!!e,e?this.on("nativecontextmenu",t.contextMenuHandler):this.removeListener("nativecontextmenu",t.contextMenuHandler)},value:!0}})}),function(){ace.require(["ace/ext/spellcheck"],function(){})}()