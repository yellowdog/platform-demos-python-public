import importlib
import os
import sys
import pathlib
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

from jupyterlab.labapp import LabApp
from jupytext.cli import jupytext
from nbformat.sign import TrustNotebookApp

demos = ["image-montage", "slurm-cluster"]


def executable(name: str) -> str:
    return os.path.join(os.path.dirname(sys.executable), name)


def call_jupyter(arguments):
    pathlib.Path("notebooks").mkdir(exist_ok=True)

    for d in demos:
        notebook = os.path.join("notebooks", f"{d}.ipynb")
        script = os.path.join("scripts", f"{d}.py")
        jupytext(["--output", notebook, script])
        TrustNotebookApp.launch_instance([notebook])
        TrustNotebookApp.clear_instance()

    set_environment(arguments)
    os.chdir("notebooks")
    LabApp.launch_instance(["--port=8888", "--no-browser", "--ip=0.0.0.0", "--ServerApp.token=''",
                            "--ServerApp.password=''", "--allow-root"])


def call_python(arguments):
    set_environment(arguments)
    importlib.import_module("scripts." + arguments.command)


def set_environment(arguments):
    namespace = arguments.namespace
    if not namespace:
        namespace = arguments.command.replace("-", "_") + "_demo"

    os.environ["URL"] = arguments.url
    os.environ["KEY"] = arguments.key
    os.environ["SECRET"] = arguments.secret
    os.environ["NAMESPACE"] = namespace
    if arguments.template_id:
        os.environ["TEMPLATE_ID"] = arguments.template_id
    os.environ["AUTO_SHUTDOWN"] = str(arguments.disable_auto_shutdown)


def add_common_arguments(argument_parser: ArgumentParser):
    argument_parser.add_argument("--url", default="https://portal.yellowdog.co/api",
                                 help="The platform URL to run against")
    argument_parser.add_argument("--key", required=True, help="The API key ID")
    argument_parser.add_argument("--secret", required=True, help="The API key secret")
    argument_parser.add_argument(
        "--namespace",
        help="The namespace to use for any compute or work. By default, a namespace will be determined for you"
    )
    argument_parser.add_argument(
        "--template-id",
        help="The compute requirement template ID to use. By default, a  dynamic template will be created for you and"
             " deleted after the demo is finished. This will select an appropriate compute source according to the"
             " needs of the demo and what you have already configured in the platform"
    )
    argument_parser.add_argument(
        "--disable-auto-shutdown",
        action='store_false',
        help="Whether to automatically shutdown any compute when the demo is finished. By default, compute will be"
             "shutdown. It can be useful to disable this if you wish to inspect the compute instances for a longer"
             "period."
    )


parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)

subparsers = parser.add_subparsers(dest="command")
subparsers.required = True

jupyter_parser = subparsers.add_parser("jupyter")
jupyter_parser.set_defaults(func=call_jupyter)
add_common_arguments(jupyter_parser)

for demo in demos:
    subparser = subparsers.add_parser(demo)
    add_common_arguments(subparser)
    subparser.set_defaults(func=call_python)

args = parser.parse_args()

os.chdir(os.path.dirname(__file__))
args.func(args)
