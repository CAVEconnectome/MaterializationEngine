import pytest
from dynamicannotationdb.annodb_meta import AnnotationMetaDB
from emannotationschemas import get_types
from pychunkedgraph.backend import chunkedgraph
import subprocess
import os
from google.auth import credentials
from google.cloud import bigtable, exceptions
from time import sleep
from signal import SIGTERM
import grpc


class DoNothingCreds(credentials.Credentials):
    def refresh(self, request):
        pass


@pytest.fixture(scope='session', autouse=True)
def bigtable_client(request):
    # setup Emulator
    bigtables_emulator = subprocess.Popen(["gcloud",
                                           "beta",
                                           "emulators",
                                           "bigtable",
                                           "start",
                                           "--host-port",
                                           "localhost:8086"],
                                          preexec_fn=os.setsid,
                                          stdout=subprocess.PIPE)

    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    startup_msg = "Waiting for BigTables Emulator to start up at {}..."
    print(startup_msg.format(os.environ["BIGTABLE_EMULATOR_HOST"]))
    c = bigtable.Client(project='emulated', credentials=DoNothingCreds(),
                        admin=True)
    retries = 5
    while retries > 0:
        try:
            c.list_instances()
        except exceptions._Rendezvous as e:
            # Good error - means emulator is up!
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                print(" Ready!")
                break
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                sleep(1)
            retries -= 1
            print(".")
    if retries == 0:
        print("\nCouldn't start Bigtable Emulator."
              " Make sure it is setup correctly.")
        exit(1)
    yield c
    # setup Emulator-Finalizer

    def fin():
        os.killpg(os.getpgid(bigtables_emulator.pid), SIGTERM)
        bigtables_emulator.wait()

    request.addfinalizer(fin)


@pytest.fixture(scope='session')
def annodb(bigtable_client):
    db = AnnotationMetaDB(client=bigtable_client, instance_id="test_instance")

    yield db


@pytest.fixture(scope='session')
def test_annon_dataset(annodb):
    amdb = annodb

    dataset_name = 'test_dataset'
    yield amdb, dataset_name


