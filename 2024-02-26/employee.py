import json
from datetime import datetime

class EmployeeTaskTracker:
    main_task_list = []

    def __init__(self, emp_name, emp_id):
        self.emp_name = emp_name
        self.emp_id = emp_id
        self.tasks = []
    def log_in(self):
        self.login_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    def add_task(self, task_title, task_description):
        task = {
            "task_title": task_title,
            "task_description": task_description,
            "start_time": datetime.now().strftime('%Y-%m-%d %H:%M'),
            "end_time": None,
            "task_success": False
        }
        self.tasks.append(task)
    def end_task(self):
        for task in self.tasks:
            task["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            task["task_success"] = True
    def log_out(self):
        self.logout_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        self._create_daily_json()
    def _create_daily_json(self):
        daily_json = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time,
            "logout_time": self.logout_time,
            "tasks": self.tasks
        }
        file_name = f"{datetime.now().strftime('%Y-%m-%d')}_{self.emp_name.replace(' ', '_')}.json"
        with open(file_name, 'w') as file:
            json.dump(daily_json, file, indent=4)
        EmployeeTaskTracker.main_task_list.append(daily_json)

if __name__ == "__main__":
    employee1 = EmployeeTaskTracker("John", 1)
    employee1.log_in()
    employee1.add_task("Task 1", "Explanation of Task 1")
    employee1.add_task("Task 2", "Explanation of Task 2")
    employee1.end_task()
    employee1.log_out()
    print(EmployeeTaskTracker.main_task_list)
