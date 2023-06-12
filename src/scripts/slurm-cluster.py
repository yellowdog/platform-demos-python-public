# ---
# jupyter:
#   kernelspec:
#     display_name: Python3
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: ".py"
#     mimetype: "text/x-python"
#     name: "python"
#     nbconvert_exporter: "python"
#     pygments_lexer: "ipython3"
#     version: "3.8.5"
# ---

# %% [markdown]
# # Configuration

# %%
import os
from datetime import timedelta

from utils.common import generate_unique_name, markdown, link, link_entity, use_template, script_relative_path, \
    get_image_family_id
from yellowdog_client import PlatformClient
from yellowdog_client.common.server_sent_events import DelegatedSubscriptionEventListener
from yellowdog_client.model import ProvisionedWorkerPoolProperties, NodeWorkerTarget, WorkerPoolNodeConfiguration, \
    NodeType, NodeSlotNumbering, NodeRunCommandAction, NodeIdFilter, NodeEvent, \
    NodeActionGroup, NodeWriteFileAction, NodeCreateWorkersAction, ComputeRequirementTemplateUsage, \
    ServicesSchema, ApiKey, ComputeRequirementDynamicTemplate, StringAttributeConstraint, WorkRequirement, TaskGroup, \
    Task, TaskOutput, RunSpecification, TaskStatus, WorkRequirementStatus, AutoShutdown

key = os.environ['KEY']
secret = os.environ['SECRET']
url = os.environ['URL']
namespace = os.environ['NAMESPACE']
template_id = os.environ.get('TEMPLATE_ID')
auto_shutdown = os.environ['AUTO_SHUTDOWN'] == "True"

slurmd_nodes = 5
tasks_per_slurmd_node = 5

run_id = generate_unique_name(namespace)

client = PlatformClient.create(ServicesSchema(defaultUrl=url), ApiKey(key, secret))

image_family_id = get_image_family_id(client, "yd-agent-slurm")

default_template = ComputeRequirementDynamicTemplate(
    name=run_id,
    strategyType='co.yellowdog.platform.model.SingleSourceProvisionStrategy',
    imagesId=image_family_id,
    constraints=[
        StringAttributeConstraint(attribute='source.provider', anyOf={'AWS'}),
        StringAttributeConstraint(attribute='source.instance-type', anyOf={"t3a.small"})
    ],
)

markdown("Configured to run against", link(url))

# %% [markdown]
# # Provision Worker Pool

# %%

slurmctl_nodes = 1
total_nodes = slurmd_nodes + slurmctl_nodes

data_file_name = "nodes.json"

with use_template(client, template_id, default_template) as template_id:
    worker_pool = client.worker_pool_client.provision_worker_pool(
        ComputeRequirementTemplateUsage(
            templateId=template_id,
            requirementNamespace=namespace,
            requirementName=generate_unique_name(namespace),
            targetInstanceCount=total_nodes,
        ),
        ProvisionedWorkerPoolProperties(
            createNodeWorkers=NodeWorkerTarget.per_node(0),
            workerTag=run_id,
            idleNodeShutdown=AutoShutdown(timeout=timedelta(0)),
            idlePoolShutdown=AutoShutdown(timeout=timedelta(0)) if auto_shutdown else AutoShutdown(enabled=False),
            nodeConfiguration=WorkerPoolNodeConfiguration(
                nodeTypes=[
                    NodeType("slurmctld", 1),
                    NodeType("slurmd", min=slurmd_nodes, slotNumbering=NodeSlotNumbering.REUSABLE)
                ],
                nodeEvents={
                    NodeEvent.STARTUP_NODES_ADDED: [
                        NodeActionGroup([
                            NodeWriteFileAction(
                                path=data_file_name,
                                content=script_relative_path('resources/startup_nodes.json.mustache').read_text(),
                                nodeTypes=["slurmctld"]
                            ),
                            NodeRunCommandAction(
                                path="start_simple_slurmctld",
                                arguments=[data_file_name],
                                environment={"EXAMPLE": "FOO"},
                                nodeTypes=["slurmctld"]
                            )
                        ]),
                        NodeActionGroup([
                            NodeRunCommandAction(
                                path="start_simple_slurmd",
                                arguments=[
                                    "{{nodesByType.slurmctld.0.details.privateIpAddress}}",
                                    "{{node.details.nodeSlot}}"
                                ],
                                nodeTypes=["slurmd"]
                            )
                        ]),
                        NodeActionGroup([
                            NodeCreateWorkersAction(
                                totalWorkers=1,
                                nodeTypes=["slurmctld"]
                            )
                        ])
                    ],
                    NodeEvent.NODES_ADDED: [
                        NodeActionGroup([
                            NodeWriteFileAction(
                                path=data_file_name,
                                content=script_relative_path('resources/added_nodes.json.mustache').read_text(),
                                nodeTypes=["slurmctld"]
                            ),
                            NodeRunCommandAction(
                                path="add_nodes",
                                arguments=[data_file_name],
                                nodeTypes=["slurmctld"]
                            )
                        ]),
                        NodeActionGroup([
                            NodeRunCommandAction(
                                nodeIdFilter=NodeIdFilter.EVENT,
                                path="start_simple_slurmd",
                                arguments=[
                                    "{{nodesByType.slurmctld.0.details.privateIpAddress}}",
                                    "{{node.details.nodeSlot}}"
                                ],
                                nodeTypes=["slurmd"]
                            )
                        ])
                    ]
                }
            )
        )
    )

markdown("Added", link_entity(url, worker_pool))

# %% [markdown]
# # Add Work Requirement

# %%

task_type = "srun"
total_tasks = tasks_per_slurmd_node * slurmd_nodes

work_requirement = client.work_client.add_work_requirement(WorkRequirement(
    namespace=namespace,
    name=generate_unique_name(namespace),
    taskGroups=[TaskGroup(
        name="tasks",
        runSpecification=RunSpecification(
            taskTypes=[task_type],
            minWorkers=1,
            maxWorkers=1,
            exclusiveWorkers=True,
            maximumTaskRetries=3,
            workerTags=[run_id]
        )
    )]
))

markdown("Added", link_entity(url, work_requirement))


# %% [markdown]
# # Add Tasks to Work Requirement

# %%


def generate_task() -> Task:
    return Task(
        name=generate_unique_name(namespace),
        taskType=task_type,
        arguments=["-N", str(slurmd_nodes), "bash", "-c", "echo Hello, world from $(hostname)!"],
        outputs=[TaskOutput.from_task_process()]
    )


tasks = [generate_task() for _ in range(total_tasks)]

client.work_client.add_tasks_to_task_group(work_requirement.taskGroups[0], tasks)

markdown("Added TASKS to", link_entity(url, work_requirement))


# %% [markdown]
# # Wait for the Work Requirement to finish


# %%

def on_update(work_req: WorkRequirement):
    completed = 0
    total = 0
    for task_group in work_req.taskGroups:
        completed += task_group.taskSummary.statusCounts[TaskStatus.COMPLETED]
        total += task_group.taskSummary.taskCount

    markdown(f"WORK REQUIREMENT is {work_req.status} with {completed}/{total} COMPLETED TASKS")


markdown("Waiting for WORK REQUIREMENT to complete...")
listener = DelegatedSubscriptionEventListener(on_update)
client.work_client.add_work_requirement_listener(work_requirement, listener)
work_requirement = client.work_client.get_work_requirement_helper(work_requirement) \
    .when_requirement_matches(lambda wr: wr.status.finished) \
    .result()

client.close()

if work_requirement.status != WorkRequirementStatus.COMPLETED:
    raise Exception("WORK REQUIREMENT did not complete. Status: " + str(work_requirement.status))

markdown(link(
    url,
    f"#/objects/{namespace}/{work_requirement.name}%2F{work_requirement.taskGroups[0].name}%2F",
    "Output is available in Object Store"
))
