import asyncio
from airflow.triggers.base import BaseTrigger, TriggerEvent
from common.airflow_management_util import *


class DagStatusCLITrigger(BaseTrigger):
    def __init__(self, dag_name, mode="success", triggerID=None):
        super().__init__()
        self.dag_name = dag_name
        self.mode = mode
        self.triggerID = triggerID

    def serialize(self):
        return ("common.deferables.trigger.task_status_cli_trigger.DagStatusCLITrigger", {"dag_name": self.dag_name, "mode": self.mode, "triggerID": self.triggerID})

    def get_tasks_status(self) -> list:
        cl = CLI(self.dag_name)
        di = cl.get_dag_last_run()
        return cl.get_dag_all_tasks_status(di['execution_date'])

    def is_all_task_success(self) -> bool:
        for di in self.get_tasks_status():
            if di['state'] != "success":
                return False
        return True

    def is_all_task_finished(self) -> bool:
        for di in self.get_tasks_status():
            if di['state'] in ["queued", "running", "up_for_retry", "up_for_reschedule", "scheduled", "deferred", "no_status"]:
                return False
        return True

    async def run(self):
        if self.mode == "success":
            while self.is_all_task_success() is False:
                await asyncio.sleep(60)
        else:
            while self.is_all_task_finished() is False:
                await asyncio.sleep(60)
        yield TriggerEvent(self.triggerID)


class TaskStatusCLITrigger(BaseTrigger):
    def __init__(self, dag_name, task_name, triggerID=None):
        super().__init__()
        self.dag_name = dag_name
        self.task_name = task_name
        self.no_exist_flag = True
        self.triggerID = triggerID

    def serialize(self):
        return ("common.deferables.trigger.task_status_cli_trigger.TaskStatusCLITrigger", {"dag_name": self.dag_name, "task_name": self.task_name, "triggerID": self.triggerID})

    def get_tasks_status(self) -> list:
        cl = CLI(self.dag_name)
        di = cl.get_dag_last_run()
        return cl.get_dag_all_tasks_status(di['execution_date'])

    def is_task_succeeded(self):
        for di in self.get_tasks_status():
            if di['task_id'] != self.task_name:
                continue
            self.no_exist_flag = False
            if di['state'] != "success":
                return False
        return True

    async def run(self):
        while self.is_task_succeeded() is False:
            if self.no_exist_flag:
                # run must yield its TriggerEvents, not return them. If it returns before yielding at least one event, Airflow will consider this an error and fail any Task Instances waiting on it. If it throws an exception, Airflow will also fail any dependent task instances.
                raise ValueError("task does not exist")
                # return TriggerEvent(self.task_name)
            await asyncio.sleep(60)
        yield TriggerEvent(self.triggerID)
        