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
import urllib.parse
from datetime import timedelta
from pathlib import Path

from yellowdog_client import PlatformClient
from yellowdog_client.common.server_sent_events import DelegatedSubscriptionEventListener
from yellowdog_client.model import ServicesSchema, ApiKey, ComputeRequirementDynamicTemplate, \
    StringAttributeConstraint, WorkRequirement, TaskGroup, RunSpecification, Task, TaskInput, TaskOutput, FlattenPath, \
    ComputeRequirementTemplateUsage, ProvisionedWorkerPoolProperties, WorkRequirementStatus, TaskStatus
from yellowdog_client.object_store.model import FileTransferStatus

from utils.common import generate_unique_name, markdown, link, link_entity, use_template, image, script_relative_path, \
    get_image_family_id

key = os.environ['KEY']
secret = os.environ['SECRET']
url = os.environ['URL']
namespace = os.environ['NAMESPACE']
template_id = os.environ.get('TEMPLATE_ID')
auto_shutdown = os.environ['AUTO_SHUTDOWN'] == "True"

run_id = generate_unique_name(namespace)

client = PlatformClient.create(
    ServicesSchema(defaultUrl=url),
    ApiKey(key, secret)
)

image_family_id = get_image_family_id(client, "yd-agent-docker")

default_template = ComputeRequirementDynamicTemplate(
    name=run_id,
    strategyType='co.yellowdog.platform.model.SingleSourceProvisionStrategy',
    imagesId=image_family_id,
    constraints=[
        StringAttributeConstraint(attribute='source.provider', anyOf={'AWS'}),
        StringAttributeConstraint(attribute='source.instanceType', anyOf={"t3a.small"})
    ],
)

markdown("Configured to run against", link(url))

# %% [markdown]
# # Upload source picture to Object Store

# %%
source_picture_path = script_relative_path("resources/ImageMontage.jpg")
source_picture_file = source_picture_path.name
client.object_store_client.start_transfers()
session = client.object_store_client.create_upload_session(namespace, str(source_picture_path))
markdown("Waiting for source picture to upload to Object Store...")
session.bind(on_error=lambda error_args: markdown(f"Error uploading file: {error_args.error_type} - {error_args.message}. {''.join(error_args.detail)}"))
session.start()
session = session.when_status_matches(lambda status: status.is_finished()).result()

if session.status != FileTransferStatus.Completed:
    raise Exception(f"Source picture failed to upload. Status: {session.status}")

stats = session.get_statistics()
markdown(link(
    url,
    f"#/objects/{namespace}/{source_picture_file}?object=true",
    f"Upload {session.status.name.lower()} ({stats.bytes_transferred}B uploaded)"
))

# %% [markdown]
# # Provision Worker Pool

# %%

with use_template(client, template_id, default_template) as template_id:
    worker_pool = client.worker_pool_client.provision_worker_pool(
        ComputeRequirementTemplateUsage(
            templateId=template_id,
            requirementNamespace=namespace,
            requirementName=run_id,
            targetInstanceCount=5
        ),
        ProvisionedWorkerPoolProperties(
            workerTag=run_id,
            nodeIdleTimeLimit=timedelta(0),
            autoShutdown=auto_shutdown
        )
    )

markdown("Added", link_entity(url, worker_pool))

# %% [markdown]
# # Add Work Requirement

# %%
image_processors_task_group_name = "ImageProcessors"
montage_task_group_name = "ImageMontage"

work_requirement = WorkRequirement(
    namespace=namespace,
    name=run_id,
    taskGroups=[
        TaskGroup(
            name=image_processors_task_group_name,
            runSpecification=RunSpecification(
                taskTypes=["docker"],
                maximumTaskRetries=3,
                minWorkers=4,
                maxWorkers=4,
                workerTags=[run_id]
            )
        ),
        TaskGroup(
            name=montage_task_group_name,
            runSpecification=RunSpecification(
                taskTypes=["docker"],
                maximumTaskRetries=3,
                workerTags=[run_id]
            ),
            dependentOn=image_processors_task_group_name,
        )
    ]
)

work_requirement = client.work_client.add_work_requirement(work_requirement)
markdown("Added", link_entity(url, work_requirement))

# %% [markdown]
# # Add Tasks to Work Requirement

# %%


def generate_task(task_name: str, conversion: str, output_file: str) -> Task:
    return Task(
        name=task_name,
        taskType="docker",
        inputs=[TaskInput.from_task_namespace(source_picture_file, True)],
        arguments=[
            "v4tech/imagemagick",
            "convert",
            conversion,
            f"/yd_working/{source_picture_file}",
            f"/yd_working/{output_file}"
        ],
        outputs=[
            TaskOutput.from_worker_directory(output_file, True),
            TaskOutput.from_task_process()
        ]
    )


montage_picture_file = "montage_" + source_picture_file

conversions = {
    "negate": "-negate",
    "paint": "-paint 10",
    "charcoal": "-charcoal 2",
    "pixelate": "-scale 2%% -scale 600x400",
    "vignette": "-background black -vignette 0x1",
    "blur": "-morphology Convolve Blur:0x25",
    "mask": "-fuzz 15%% -transparent white -alpha extract -negate"
}

client.work_client.add_tasks_to_task_group_by_name(
    namespace,
    work_requirement.name,
    image_processors_task_group_name,
    [generate_task(k + "_image", v, k + "_" + source_picture_file) for k, v in conversions.items()]
)

montage_task_name = "MontageImage"
image_montage_tasks = [
    Task(
        name=montage_task_name,
        taskType="docker",
        inputs=[
            TaskInput.from_task_namespace(source_picture_file),
            TaskInput.from_task_namespace(f"{work_requirement.name}/**/*_{source_picture_file}")
        ],
        flattenInputPaths=FlattenPath.FILE_NAME_ONLY,
        arguments=[
            "v4tech/imagemagick", "montage", "-geometry", "450",
            f"/yd_working/{source_picture_file}",
            *[f"/yd_working/{k}_{source_picture_file}" for k, v in conversions.items()],
            f"/yd_working/{montage_picture_file}",
        ],
        outputs=[
            TaskOutput.from_worker_directory(montage_picture_file, required=True),
            TaskOutput.from_task_process()
        ]
    )
]

client.work_client.add_tasks_to_task_group_by_name(namespace, work_requirement.name, montage_task_group_name, image_montage_tasks)

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
client.work_client.remove_work_requirement_listener(listener)
if work_requirement.status != WorkRequirementStatus.COMPLETED:
    raise Exception("WORK REQUIREMENT did not complete. Status " + str(work_requirement.status))

# %% [markdown]
# # Download result of Work Requirement

# %%

output_path = Path("out").resolve()
output_path.mkdir(parents=True, exist_ok=True)

markdown("Waiting for output picture to download from Object Store...")
output_object = f"{work_requirement.name}/{montage_task_group_name}/{montage_task_name}/{montage_picture_file}"
session = client.object_store_client\
    .create_download_session(namespace, output_object, str(output_path), montage_picture_file)
session.bind(on_error=lambda error_args: markdown(f"Error uploading file: {error_args.error_type} - {error_args.message}. {''.join(error_args.detail)}"))
session.start()
session = session.when_status_matches(lambda status: status.is_finished()).result()

if session.status != FileTransferStatus.Completed:
    raise Exception(f"Output picture failed to download. Status: {session.status}")

stats = session.get_statistics()
markdown(f"Download {session.status.name.lower()} ({stats.bytes_transferred}B downloaded)")

markdown(image(str(output_path / montage_picture_file), "The final picture"))
markdown("It can also be accessed via the Portal at:", link(url, f"#/objects/{namespace}/{urllib.parse.quote_plus(output_object)}?object=true"))

client.close()
