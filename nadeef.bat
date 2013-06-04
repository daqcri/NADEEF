:begin
@echo off

:JDK_CHECK
call :ProgInPath javac.exe
if "%PROG%" == "" (
    goto noJDK
) else (
    echo.Use Java from %PROG%
    goto START
)

:COMPILE_CHECK
if not exist "out\nadeef.jar" goto noCompile

:START
set BuildVersion=1.0.974
call :ProgInPath java.exe
"%PROG%" -d64 -Xmx2048M -cp out\nadeef.jar;out\test;examples\ qa.qcri.nadeef.console.Console
goto :eof

:noJDK
@echo on
echo Cannot find JDK installed, Please specify JAVA_HOME.
goto :eof

:noCompile
echo Nadeef is not compiled yet. Run 'ant' to compile Nadeef.
goto :eof

:ProgInPath
set PROG=%~$PATH:1
goto :eof
