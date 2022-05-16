from airflow.sensors.base import BaseSensorOperator
from common.deferables.trigger.task_status_cli_trigger import DagStatusCLITrigger, TaskStatusCLITrigger


class DagSensorCLIOperator(BaseSensorOperator):
    """ 
    DagSensorCLIOperator(
            task_id=f'cli_dagsensor_{target_dag}',
            dag_name=target_dag,
            dag=dag  
        )
    """
    def __init__(self,
                 dag_name,
                 *args,
                 **kwargs):
        super(DagSensorCLIOperator, self).__init__(*args, **kwargs)
        self.dag_name = dag_name

    def execute(self, context):
        self.defer(trigger=DagStatusCLITrigger(self.dag_name, triggerID=context['task_instance_key_str']), method_name="resume_method")
 
    def resume_method(self, context, event=None):
        return


class TaskSensorCLIOperator(BaseSensorOperator):
    """
    TaskSensorCLIOperator(
                task_id=f'cli_tasksensor_{target_dag}_{task}',
                dag_name=target_dag,
                task_name=task,
                dag=dag  
            )
    """
    def __init__(self,
                 dag_name,
                 task_name,
                 *args,
                 **kwargs):
        super(TaskSensorCLIOperator, self).__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.task_name = task_name

    def execute(self, context):
        self.defer(trigger=TaskStatusCLITrigger(self.dag_name, self.task_name, context['task_instance_key_str']), method_name="resume_method")
 
    def resume_method(self, context, event=None):
        return