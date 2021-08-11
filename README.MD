# Platform Demos - Python

These examples demonstrate how to use the YellowDog Python SDK to interact with the YellowDog Platform.

There are 2 demos available:

* image-montage: A small, embarrassingly parallel workload is created and executed on Platform provisioned compute.
* slurm-cluster: A dynamic slurm cluster is provisioned and given a trivial hello world workload

These demos can be run in 2 different ways:

* As single Python scripts
* Interactively as Jupyter Notebooks

Additionally, they may be executed directly on your host machine (helpful if you already have the right version of
Python installed), or via Docker (easier to run if you already have Docker installed).

It is recommended that you try Docker for the easiest experience.

## Setup

You will need to ensure you have configured the following via the YellowDog Platform Portal:

* Added a credential for the cloud provider from whom you wish to provision compute
* Created a compute source template for the same cloud provider and configured to use that credential
* Created an application with access to the above credential and noted down its API key ID and secret.

## Parameters

Regardless of whether you run on your machine or in a Docker container, you will be prompted to select a command and
then specify the required parameters. The format of the parameters are identical regardless of which method you are
using.

The available commands are:

* image-montage - run the image-montage demo script
* slurm-cluster - run the slurm-cluster demo script
* jupyter - spin up a Jupyter Notebook server from where you can run any of the above demos interactively

You will also at least need to supply the application key ID (`--key`) and secret (`--secret`).

Optionally, you may want to override the URL (`--url`) of the YellowDog platform you are using as by default, it will
point at our production SAAS offering i.e. https://portal.yellowdog.co/api.

## Running on Docker

Run with:

    docker run -p8888:8888 yellowdogco/platform-demos-python-public COMMAND --key KEY --secret SECRET [--url URL]

For example:

    docker run -p8888:8888 yellowdogco/platform-demos-python-public image-montage --key foo --secret bar
    docker run -p8888:8888 yellowdogco/platform-demos-python-public jupyter --key foo --secret bar --url https://foo.yellowdog.co/api

## Running on your machine

First install all dependencies:

    python3 -m pip install -r requirements.txt

Then run with:

    python3 -u src/main.py COMMAND --key KEY --secret SECRET [--url URL]

For example:

    python3 -u src/main.py image-montage --key foo --secret bar
    python3 -u src/main.py jupyter --key foo --secret bar --url https://foo.yellowdog.co/api

## Making changes

If you want alter the demos, you can edit the source directly and run with Python on your machine.

If you want to continue to work with docker, you must first build a docker image like this:

    DOCKER_BUILDKIT=1 docker build -t platform-demos-python-custom .

This will create a local docker image that you can now run. You just need to replace
`yellowdogco/platform-demos-python-public` with `platform-demos-python-custom` in the examples above.
