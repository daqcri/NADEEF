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

ace.define("ace/snippets/velocity",["require","exports","module"],function(e,t,n){t.snippetText='# macro\nsnippet #macro\n	#macro ( ${1:macroName} ${2:\\$var1, [\\$var2, ...]} )\n		${3:## macro code}\n	#end\n# foreach\nsnippet #foreach\n	#foreach ( ${1:\\$item} in ${2:\\$collection} )\n		${3:## foreach code}\n	#end\n# if\nsnippet #if\n	#if ( ${1:true} )\n		${0}\n	#end\n# if ... else\nsnippet #ife\n	#if ( ${1:true} )\n		${2}\n	#else\n		${0}\n	#end\n#import\nsnippet #import\n	#import ( "${1:path/to/velocity/format}" )\n# set\nsnippet #set\n	#set ( $${1:var} = ${0} )\n',t.scope="velocity",t.includeScopes=["html","javascript","css"]})