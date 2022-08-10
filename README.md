# dask-lxplus

Builds on top of Dask-Jobqueue to enable jobs to run on the CERN HTCondor cluster via LXPLUS.

## Summary

```python
from distributed import Client 
from dask_lxplus import CernCluster
import socket

cluster = CernCluster(
    cores = 1,
    memory = '2000MB',
    disk = '10GB',
    death_timeout = '60',
    lcg = True,
    nanny = False,
    container_runtime = 'none',
    log_directory = '/eos/user/b/ben/condor/log',
    scheduler_options = {
        'port': 8786,
        'host': socket.gethostname(),
    },
    job_extra = {
        'MY.JobFlavour': '"longlunch"',
    },
)
```

## CERN extras
There are a few changes in the wrapper to address some of the particular features of the CERN 
HTCondor cluster, but there are also a few changes to detail here.

### Options
`lcg`: If set to `True` this will validate and use the LCG python environment per the managed [LCG](https://lcgdocs.web.cern.ch/lcgdocs/lcgreleases/introduction/) 
releases. It will send the environment of the submitting scheduler to the batch worker node. DASK 
normally requires that both the scheduler and the worker is the same python versions and libraries. 
At CERN this would mean that you should, assuming say the default of `CentOS7` worker nodes, that 
the scheduler is run on something like`lxplus.cern.ch`also running CentOS7`. 
An example use would be to do the following before running dask:
```bash
$ . /cvmfs/sft.cern.ch/lcg/views/LCG_102/x86_64-centos7-gcc11-opt/setup.sh
```

`container_runtime`: Can be set to `"singularity"` or `docker` or `"none"`. If a runtime is needed 
for the worker, this selects which will be used for the `HTCondor` job the worker runs. In principle 
it should not be necessary when using `lcg` and should therefore be set to `"none"`. Default though 
is `"singularity"`.

`worker_image`: The image that will be used if `container_runtime` is defined to use one. The default 
is defined in `jobqueue-cern.yaml`.

`batch_name`: Optionally set a string that will identify the jobs in `HTCondor`. The default is 
`"dask-worker"`
