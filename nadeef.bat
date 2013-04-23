:begin
@echo off

if not exist "out\production" goto noCompile

java -cp out\nadeef.jar qa.qcri.nadeef.console.Console
goto end

:noCompile
echo Nadeef is not compiled yet. Run 'ant all' to compile Nadeef.

:end
