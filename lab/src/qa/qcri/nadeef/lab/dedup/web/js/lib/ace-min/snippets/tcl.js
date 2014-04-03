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

ace.define("ace/snippets/tcl",["require","exports","module"],function(e,t,n){t.snippetText="# #!/usr/bin/env tclsh\nsnippet #!\n	#!/usr/bin/env tclsh\n	\n# Process\nsnippet pro\n	proc ${1:function_name} {${2:args}} {\n		${3:#body ...}\n	}\n#xif\nsnippet xif\n	${1:expr}? ${2:true} : ${3:false}\n# Conditional\nsnippet if\n	if {${1}} {\n		${2:# body...}\n	}\n# Conditional if..else\nsnippet ife\n	if {${1}} {\n		${2:# body...}\n	} else {\n		${3:# else...}\n	}\n# Conditional if..elsif..else\nsnippet ifee\n	if {${1}} {\n		${2:# body...}\n	} elseif {${3}} {\n		${4:# elsif...}\n	} else {\n		${5:# else...}\n	}\n# If catch then\nsnippet ifc\n	if { [catch {${1:#do something...}} ${2:err}] } {\n		${3:# handle failure...}\n	}\n# Catch\nsnippet catch\n	catch {${1}} ${2:err} ${3:options}\n# While Loop\nsnippet wh\n	while {${1}} {\n		${2:# body...}\n	}\n# For Loop\nsnippet for\n	for {set ${2:var} 0} {$$2 < ${1:count}} {${3:incr} $2} {\n		${4:# body...}\n	}\n# Foreach Loop\nsnippet fore\n	foreach ${1:x} {${2:#list}} {\n		${3:# body...}\n	}\n# after ms script...\nsnippet af\n	after ${1:ms} ${2:#do something}\n# after cancel id\nsnippet afc\n	after cancel ${1:id or script}\n# after idle\nsnippet afi\n	after idle ${1:script}\n# after info id\nsnippet afin\n	after info ${1:id}\n# Expr\nsnippet exp\n	expr {${1:#expression here}}\n# Switch\nsnippet sw\n	switch ${1:var} {\n		${3:pattern 1} {\n			${4:#do something}\n		}\n		default {\n			${2:#do something}\n		}\n	}\n# Case\nsnippet ca\n	${1:pattern} {\n		${2:#do something}\n	}${3}\n# Namespace eval\nsnippet ns\n	namespace eval ${1:path} {${2:#script...}}\n# Namespace current\nsnippet nsc\n	namespace current\n",t.scope="tcl"})