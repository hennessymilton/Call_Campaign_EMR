set Usersname=ARoethe
set venv=emrr

call C:\Users\%Usersname%\Anaconda3\Scripts\activate C:\Users\%Usersname%\Anaconda3 
call activate base

call conda env create --file emrr.yml
call activate %venv%