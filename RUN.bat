set Usersname=ARoethe
set venv=emrr
set folder=C:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/EMR Remote/Call_Campaign_EMR
:: -------------------------------------------------------------------------
:: Open anaconda & activate env
:: -------------------------------------------------------------------------
call C:\Users\%Usersname%\Anaconda3\Scripts\activate C:\Users\%Usersname%\Anaconda3 
call activate CallAutomation
:: -------------------------------------------------------------------------
:: Change directory for relative path thats needed for the script
:: -------------------------------------------------------------------------
cd %folder%
:: -------------------------------------------------------------------------
:: Run script at this location
:: -------------------------------------------------------------------------
call C:/Users/%Usersname%/Anaconda3/envs/%venv%/python.exe "%folder%/CC.py"
PAUSE

