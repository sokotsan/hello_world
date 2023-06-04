SET log_file=D:\DASH\py_Auto\DOPD_collect_MOTR.txt

call D:\PYTHON\Scripts\activate.bat
python D:\Program\PYTHON_SERVER\Task\TSK\CollectMOTR.py > %log_file% 
