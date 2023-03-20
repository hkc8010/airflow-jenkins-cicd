from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

class CustomBashOperator(BashOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor_config:dict={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={
                                    "cpu": 0.5,
                                    "memory": "500Mi",
                                    "ephemeral-storage": "1Gi"
                                },
                                limits={
                                    "cpu": 0.5,
                                    "memory": "500Mi",
                                    "ephemeral-storage": "1Gi"
                                }
                            )
                        )
                    ]
                )
            )
        }

    def execute(self, context):
        super().execute(context)
