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
set argC=0
for %%x in (%*) do Set /A argC+=1

if not %argC% == 1 (
    goto usage
) else (
    if "%1" == "console" (
        "%JAVA_HOME%\bin\java -d64 -Dderby.drda.startNetworkServer=true -cp out\bin\*;out\test;examples\ qa.qcri.nadeef.console.Console"
    ) else if "%1" == "dashboard" (
        "%JAVA_HOME%\bin\java -d64 -cp out/bin/*:. qa.qcri.nadeef.web.NadeefStart"
    ) else (
        goto usage
    )
)

goto end

:noJDK
echo Cannot find JDK installed.
goto end

:noCompile
echo Nadeef is not compiled yet. Run 'ant' to compile Nadeef.

:usage
echo Usage: nadeef.sh [OPTIONS]
echo Options are:
echo     console : start the NADEEF console.
echo     dashboard : start the NADEEF dashboard.

:end
