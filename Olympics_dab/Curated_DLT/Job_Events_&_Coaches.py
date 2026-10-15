# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Job_Events_Coaches = Job.from_dict(
    {
        "name": "Job Events & Coaches",
        "tasks": [
            {
                "task_key": "Lookup",
                "notebook_task": {
                    "notebook_path": "/Workspace/Olympics/Array_Parameter",
                    "source": "WORKSPACE",
                },
                "environment_key": "Lookup_environment",
            },
            {
                "task_key": "SilverEntitiesLoop",
                "depends_on": [
                    {
                        "task_key": "Lookup",
                    },
                ],
                "for_each_task": {
                    "inputs": "{{tasks.Lookup.values.my_output}}",
                    "task": {
                        "task_key": "SilverEntitiesLoop_iteration",
                        "notebook_task": {
                            "notebook_path": "/Workspace/Olympics/Silver_Entities_Coaches&events",
                            "base_parameters": {
                                "source_container": "{{input.source_container}}",
                                "sink_container": "{{input.sink_container}}",
                                "folder": "{{input.folder}}",
                            },
                            "source": "WORKSPACE",
                        },
                    },
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "environments": [
            {
                "environment_key": "Lookup_environment",
                "spec": {
                    "environment_version": "6",
                },
            },
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Job_Events_Coaches, job_id=1003888748784892)
# or create a new job using: w.jobs.create(**Job_Events_Coaches.as_shallow_dict())
