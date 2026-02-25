@echo off
REM Instala o agente como serviço do Windows usando NSSM.
REM Ajuste os caminhos abaixo antes de executar como Administrador.

set PY_EXE=C:\Python313\python.exe
set AGENT_PATH=C:\MonitorProd\agent\agent.py
set SERVICE_NAME=MonitorProdAgent
set WORKDIR=C:\MonitorProd\agent

REM Caminho do nssm.exe (adicione à PATH ou informe o caminho completo)
set NSSM=nssm

%NSSM% install %SERVICE_NAME% "%PY_EXE%" "%AGENT_PATH%"
%NSSM% set %SERVICE_NAME% AppDirectory "%WORKDIR%"
%NSSM% set %SERVICE_NAME% AppExit Default Restart
%NSSM% set %SERVICE_NAME% Start SERVICE_AUTO_START

echo Servico instalado. Para iniciar:
echo   %NSSM% start %SERVICE_NAME%
