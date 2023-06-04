SET log_file=D:\DASH\py_Auto\NOAA.txt

call D:\PYTHON\Scripts\activate.bat
python D:\DASH\py_Code\NOAA.py arg1 arg2 > %log_file% 
