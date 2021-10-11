set venv=emrr
:: -------------------------------------------------------------------------
:: Open anaconda & activate env
:: -------------------------------------------------------------------------
call %USERPROFILE%\Anaconda3\Scripts\activate %USERPROFILE%\Anaconda3 
call activate %venv%
:: -------------------------------------------------------------------------
:: Change directory for relative path thats needed for the script
:: -------------------------------------------------------------------------
cd %~dp0
:: -------------------------------------------------------------------------
:: Run script at this location
:: -------------------------------------------------------------------------
call %USERPROFILE%/Anaconda3/envs/%venv%/python.exe "%~dp0\main.py"
PAUSE

