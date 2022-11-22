import contextlib
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

from IPython import get_ipython
from IPython.display import display, Markdown
from yellowdog_client import PlatformClient
from yellowdog_client.model import ComputeRequirementTemplate, WorkRequirement, ComputeRequirement, \
    ConfiguredWorkerPool, ProvisionedWorkerPool, MachineImageFamilySearch


def generate_unique_name(prefix: str) -> str:
    return (prefix + "-" + str(uuid.uuid4()))[:50]


@contextlib.contextmanager
def use_template(
        client: PlatformClient,
        template_id: Optional[str] = None,
        template: Optional[ComputeRequirementTemplate] = None
):
    if template_id:
        yield template_id
    else:
        template = client.compute_client.add_compute_requirement_template(template)
        try:
            yield template.id
        finally:
            client.compute_client.delete_compute_requirement_template(template)


def get_image_family_id(client: PlatformClient, image_family: str) -> str:
    image_families = client.images_client.search_image_families(MachineImageFamilySearch(
        includePublic=True,
        namespace="yellowdog",
        familyName=image_family
    ))

    image_families = [i for i in image_families if i.name == image_family]

    if not image_families:
        raise Exception("Unable to find ID for image family: " + image_family)
    elif len(image_families) > 1:
        raise Exception("Multiple matching image families found")

    return image_families[0].id


def camel_case_split(value: str) -> str:
    return " ".join(re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', value))


console_supports_markdown = get_ipython().__class__.__name__ == "ZMQInteractiveShell"


@dataclass
class Output:
    text: str
    render: bool = True


def markdown(*args: Union[str, Output]) -> None:
    if console_supports_markdown:
        display(Markdown(" ".join(args)))
    else:
        print(*args)


def link(base_url: str, url_suffix: str = "", text: Optional[str] = None) -> str:
    url_parts = urlparse(base_url)
    base_url = url_parts.scheme + "://" + url_parts.netloc

    url = base_url + "/" + url_suffix

    if not text:
        text = url

    if console_supports_markdown:
        return '[%s](%s)' % (text, url)
    elif text == url:
        return url
    else:
        return '%s (%s)' % (text, url)


def image(path: str, text: Optional[str] = None) -> str:
    if not text:
        text = path

    if console_supports_markdown:
        return '![%s](%s)' % (text, path)
    elif text == path:
        return path
    else:
        return '%s has been downloaded to: %s' % (text, path)


entities = {
    ConfiguredWorkerPool: "workers",
    ProvisionedWorkerPool: "workers",
    WorkRequirement: "work",
    ComputeRequirement: "compute"
}


def link_entity(base_url: str, entity: object) -> str:
    entity_type = type(entity)
    return link(
        base_url,
        "#/%s/%s" % ((entities.get(entity_type)), entity.id),
        camel_case_split(entity_type.__name__).upper()
    )


def script_relative_path(path: str) -> Path:
    return Path(__file__).parents[1] / Path(path)
