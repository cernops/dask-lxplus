import os
import unittest
from unittest.mock import patch
from dask_lxplus.cluster import check_job_script_prologue
from dask_lxplus.cluster import get_xroot_url
from dask_lxplus import CernCluster


class TestCluster(unittest.TestCase):

    def test_job_script_prologue(self):
        job_script_prologue = ["export PYTHONHOME=/usr/local/bin/python", 'export LD_LIBRARY_PATH="/usr/local/lib"']
        self.assertFalse(check_job_script_prologue("PATH", []))
        self.assertTrue(check_job_script_prologue("PYTHONHOME", job_script_prologue))
        self.assertTrue(check_job_script_prologue("LD_LIBRARY_PATH", job_script_prologue))
        self.assertFalse(check_job_script_prologue("PATH", job_script_prologue))

    def test_xroot_url(self):
        for p in ["/eos/user/b/bejones/SWAN_projects", "/eos/home-b/bejones/SWAN_projects", "/eos/home-io3/b/bejones/SWAN_projects"]:
            self.assertEqual(get_xroot_url(p), "root://eosuser.cern.ch//eos/user/b/bejones/SWAN_projects", f"{p}")

    def test_submit_command_extra(self):
        with CernCluster(
            cores = 4,
            processes = 2,
            memory = "2000MB",
            disk = "1000MB",
        ) as cluster:
            self.assertIn("-spool", cluster.new_spec['options']['submit_command_extra'])


    def test_job_script_singularity(self):
        with CernCluster(
            cores = 4,
            processes = 2,
            memory = "2000MB",
            disk = "1000MB",
            container_runtime = "singularity",
            job_extra_directives = {
                "MY.Jobflavour": '"longlunch"',
            },
        ) as cluster:
            job_script = cluster.job_script()
            self.assertIn('MY.Jobflavour = "longlunch"', job_script)
            self.assertIn('MY.SingularityImage = "/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-cc7:latest"', job_script)


    def test_job_script_docker(self):
        with CernCluster(
            cores = 4,
            processes = 2,
            memory = "2000MB",
            disk = "1000MB",
            container_runtime = "docker",
            worker_image = "dask-lxplus/lxdask-cc7:latest",
            job_extra_directives = {
                "MY.Jobflavour": '"longlunch"',
            },
        ) as cluster:
            job_script = cluster.job_script()
            self.assertIn('MY.Jobflavour = "longlunch"', job_script)
            self.assertIn('universe = docker', job_script)
            self.assertIn('docker_image = "dask-lxplus/lxdask-cc7:latest"', job_script)

    @patch("sys.executable", "/cvmfs/sft-nightlies.cern.ch/lcg/views/devswan/Mon/x86_64-centos7-gcc8-opt/bin/python3")
    @patch.dict(os.environ, {"SERVER_HOSTNAME": "swan.cern.ch",
                             "PYTHONHOME": "/cvmfs/sft.cern.ch/lcg/releases/Python/3.9.6-b0f98/x86_64-centos7-gcc8-opt",
                             "LD_LIBRARY_PATH": "/cvmfs/sft.cern.ch/lcg/releases/Python/3.9.6-b0f98/x86_64-centos7-gcc8-opt/lib",
                             "PATH": "/cvmfs/sft-nightlies.cern.ch/lcg/views/devswan/Mon/x86_64-centos7-gcc8-opt/scripts:/cvmfs/sft-nightlies.cern.ch/lcg/views/devswan/Mon/x86_64-centos7-gcc8-opt/bin:",
                             "PYTHONPATH": "/cvmfs/sft-nightlies.cern.ch/lcg/latest/condor/8.9.11-b3c6e/x86_64-centos7-gcc8-opt/lib/python3",
                             "ROOT_INCLUDE_PATH": "/cvmfs/sft-nightlies.cern.ch/lcg/views/devswan/Mon/x86_64-centos7-gcc8-opt/include/Geant4",
                             })
    def test_job_script_lcg(self):
        with CernCluster(
                cores=4,
                processes=4,
                memory="2000MB",
                disk="1000MB",
                lcg=True,
                env_extra=["FOO=BAR, PATH=/one/two/three:/four/five/six"],
        ) as cluster:
            job_script = cluster.job_script()
            self.assertIn('FOO=BAR', job_script)
            self.assertIn('PATH=/one/two/three:/four/five/six', job_script)
            self.assertIn('getenv = true', job_script)


if __name__ == '__main__':
    unittest.main()
