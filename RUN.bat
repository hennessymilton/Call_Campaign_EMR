:: -------------------------------------------------------------------------
:: Open anaconda & activate env
:: -------------------------------------------------------------------------
call C:\Users\ARoethe\Anaconda3\Scripts\activate C:\Users\ARoethe\Anaconda3 
call activate CallAutomation

:: -------------------------------------------------------------------------
:: Change directory for relative path thats needed for the script
:: -------------------------------------------------------------------------
cd C:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/EMR Remote/Call_Campaign_EMR

:: -------------------------------------------------------------------------
:: Run script at this location
:: -------------------------------------------------------------------------
call C:/Users/ARoethe/Anaconda3/envs/CallAutomation/python.exe "c:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/EMR Remote/Call_Campaign_EMR/CC.py"
::call C:/Users/ARoethe/Anaconda3/envs/CallAutomation/python.exe "c:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/Call Campaign Automation/Call Campaign Automation Tool/test.py"
PAUSE

