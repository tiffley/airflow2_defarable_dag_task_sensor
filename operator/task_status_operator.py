from airflow.sensors.base import BaseSensorOperator
from common.deferables.trigger.task_status_trigger import DagStatusTrigger, TaskStatusTrigger
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.db import provide_session


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

    @provide_session
    def execute(self, context, session=None):
        tg_dag = SerializedDagModel.get(self.dag_name, session).dag
        self.defer(trigger=DagStatusTrigger(self.dag_name, execution_date=tg_dag.get_latest_execution_date(), triggerID=context['task_instance_key_str']), method_name="resume_method")
 
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

    @provide_session
    def execute(self, context, session=None):
        tg_dag = SerializedDagModel.get(self.dag_name, session).dag
        self.defer(trigger=TaskStatusTrigger(self.dag_name, self.task_name, execution_date=tg_dag.get_latest_execution_date(), triggerID=context['task_instance_key_str']), method_name="resume_method")
 
    def resume_method(self, context, event=None):
        return