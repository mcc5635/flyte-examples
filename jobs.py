from flytekit import task, workflow

@task
def hello_world(name: str) -> str:
    return f"hello {name}"

@task
def add_exclamation_mark(text: str) -> str:
    return f"{text}!"

@workflow
def hello_world_df(name: str = "KubeCon") -> str:
    greeting = hello_world(name=name)
    return add_exclamation_mark(text=greeting)

if __name__ == "__name__":
   hello_world(name="Joe")


# Resource Provisioning Example
   
from flytekit import task, Resources
from flytekit.extras.accelerators import GPUAccelerator

@task(
    requests=Resources(cpu="4", mem="16Gi", gpu="1"),
    limits=Resources(cpu="8", mem="32Gi", gpu="1"),
    accelerator=GPUAccelerator("nvidia-tesla-v100"),
)
def large_workload() -> str:
    return "large operation"


# Spot Instance example (contrasted to On-Demand)
from flytekit import task

@task(
    interruptible=True,
)
def spot_workload() -> str:
    return "resilient task"

# Cache task example

from flytekit import task

@task(
    cache=True,
    cache_version="1.0",
)
def expensive_workload() -> str:
    return "expensive task"


# Integrations: Kubernetes Plugins

from distributed import Client
from flytekitplugins.dask import Dask, Scheduler, WorkerGroup

from flytekit import task, Resources

@task(
    task_config=Dask(
        workers=WorkerGroup(
            number_of_workers=10,
            requests=Resources(cpu="4", mem="16Gi"),
        ),
    )
)
def add_ones(values: list[int]) -> list[int]:
    with Client() as client:
        futures = client.map(lambda x: x + 1, values)
        return client.gather(futures)
