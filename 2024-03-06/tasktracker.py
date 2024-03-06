import json
from datetime import datetime
from pymongo import MongoClient

class EmployeeTaskTracker:
    client = MongoClient('localhost', 27017)  # Connect to MongoDB
    db = client['employee_tasktracker']  # Create or connect to the 'employee_tasks' database
    collection = db['tasks']  # Create or connect to the 'tasks' collection

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
        self._save_to_mongodb()

    def _save_to_mongodb(self):
        current_date = datetime.now().strftime('%Y-%m-%d')

        collection_name = f"{self.emp_name.replace(' ', '')}_{self.emp_id}"

        collection = self.db[collection_name]

        # Create the data dictionary
        data = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time,
            "logout_time": self.logout_time,
            "tasks": self.tasks
        }
        collection.insert_one(data)

if __name__ == "__main__":
    employee1 = EmployeeTaskTracker("Denni", 2)
    employee1.log_in()
    employee1.add_task("Task 1", "Explanation of Task 1")
    employee1.add_task("Task 2", "Explanation of Task 2")
    employee1.end_task()
    employee1.log_out()
