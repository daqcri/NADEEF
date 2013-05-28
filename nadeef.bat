:begin
@echo off

:JDK_CHECK
if defined JAVA_HOME (
if not exist "%JAVA_HOME%\bin\javac.exe" goto noJDK
)

:COMPILE_CHECK
if not exist "out\nadeef.jar" goto noCompile

:START
"%JAVA_HOME%\bin\java" -d64 -Xmx2048M -cp out\nadeef.jar;out\test qa.qcri.nadeef.console.Console
goto end

:noJDK
echo Cannot find JDK installed.
goto end

:noCompile
echo Nadeef is not compiled yet. Run 'ant' to compile Nadeef.

:end
