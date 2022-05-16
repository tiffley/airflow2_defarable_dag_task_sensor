from airflow.sensors.base import BaseSensorOperator
from common.deferables.trigger.task_status_trigger import DagStatusTrigger, TaskStatusTrigger


class DagSensorOperator(BaseSensorOperator):
    """ 
    DagSensorOperator(
            task_id=f'dagsensor_{target_dag}',
            dag_name=target_dag,
            dag=dag  
        )  
    """
    def __init__(self,
                 dag_name: str,
                 *args,
                 **kwargs):
        super(DagSensorOperator, self).__init__(*args, **kwargs)
        self.dag_name = dag_name

    def execute(self, context):
        self.defer(trigger=DagStatusTrigger(self.dag_name, context['task_instance_key_str']), method_name="resume_method")
 
    def resume_method(self, context, event=None):
        return


class TaskSensorOperator(BaseSensorOperator):
    """
        TaskSensorOperator(
                task_id=f'tasksensor_{target_dag}_{task}',
                dag_name=target_dag,
                task_name=task,
                dag=dag  
            )
    """
    def __init__(self,
                 task_name: str,
                 dag_name: str,
                 *args,
                 **kwargs):
        super(TaskSensorOperator, self).__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.task_name = task_name

    def execute(self, context):
        self.defer(trigger=TaskStatusTrigger(self.dag_name, self.task_name, context['task_instance_key_str']), method_name="resume_method")
 
    def resume_method(self, context, event=None):
        return