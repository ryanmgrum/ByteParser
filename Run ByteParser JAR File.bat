@echo off
setlocal
set jar="D:\ryan\documents\IntelliJ IDEA\ByteParser\out\artifacts\ByteParser_jar\ByteParser.jar"
if "%~1" equ "" goto :eof

:loop
java -jar %jar% "%~f1"

shift
if "%~1" neq "" goto :loop