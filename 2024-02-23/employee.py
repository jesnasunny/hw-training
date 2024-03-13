import json
import datetime

class EmployeeTaskLogger:
    maintask_list=[]

    def __init__(self,emp_id,emp_name):
        self.emp_name=emp_name,
        self.emp_id=emp_id,
	    self.login_time=None,
	    self.logout_time=None,
	    self.tasks=[]
    
    def log_in(self):
        self.login_time=datetime.now().strftime("%Y-%m-%d %H:%M")

    def log_out:
        self.logout_time=datetime.now().strftime("%Y-%m-%d %H:%M")
        self.create_daily_json()
    def add_task(self,task_title,task_description,start_time,end_time,task_success):
        task_title: "Title of the task",
	    task_description: "Detailed explanation of the task",
	    start_time :start_time,
	    end_time :start_time,
	    task_success:task_success
