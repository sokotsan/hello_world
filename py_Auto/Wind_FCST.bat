SET log_file=D:\DASH\py_Auto\Wind_FCST.txt
SET log_file2=D:\DASH\py_Auto\emailer.txt
SET log_file3=D:\DASH\py_Auto\Versify.txt
call D:\PYTHON\Scripts\activate.bat
python D:\DASH\py_Code\Meteor.py arg1 arg2 > %log_file% 
python D:\DASH\py_Code\Versify.py arg1 arg2 > %log_file3% 
python D:\DASH\py_Code\send_email_Meteor.py arg1 arg2 > %log_file2% 