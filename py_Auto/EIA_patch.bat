SET log_file=D:\DASH\py_Auto\EIA_patch.log
SET log_file2=D:\DASH\py_Auto\EIA_patch2.log
call D:\Program\PYTHON_SERVER\activate.bat
python D:\DASH\py_Code\EIA_BA_BA_FIX.py >%log_file%
python D:\DASH\py_Code\EIA_BA_Gen_FIX.py >%log_file2%
