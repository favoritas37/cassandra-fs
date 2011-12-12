@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off
SETLOCAL

if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%
if NOT DEFINED CASSANDRA_CONF set CASSANDRA_CONF=%CASSANDRA_HOME%\conf

SET CASSANDRA_LIBS=%CASSANDRA_HOME%\lib

FOR %%a IN (%CASSANDRA_HOME%\lib\*.jar) DO  call :append %%~fa
@REM java -Xmx1024m -cp %CASSANDRA_LIBS%;%CASSANDRA_HOME%\build\apache-cassandra-fs-0.6.0.jar;%CASSANDRA_HOME%\conf -Dstorage-config=%CASSANDRA_HOME%/conf -Dfile.encoding=UTF-8 org.apache.cassandra.contrib.fs.cli.FSCliMain
java -jar %CASSANDRA_HOME%\dist\CassandraFsClient.jar

:append
SET CASSANDRA_LIBS=%CASSANDRA_LIBS%;%1%2
goto :finally


:finally

ENDLOCAL
