:begin
@echo off

:JDK_CHECK
if defined JAVA_HOME (
if not exist "%JAVA_HOME%\bin\javac.exe" goto noJDK
)

:COMPILE_CHECK
if not exist "out\bin\nadeef.jar" goto noCompile

:START
set BuildVersion=1.0.1019
"%JAVA_HOME%\bin\java" -d64 -Xmx2048M -Dderby.drda.startNetworkServer=true -cp out\bin\*;out\test;examples\ qa.qcri.nadeef.console.Console
goto end

:noJDK
echo Cannot find JDK installed.
goto end

:noCompile
echo Nadeef is not compiled yet. Run 'ant' to compile Nadeef.

:end
