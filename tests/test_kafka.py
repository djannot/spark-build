import logging
import os
import pytest
import json
import shakedown
import subprocess

from tests import s3, utils

import sdk_auth
import sdk_hosts
import sdk_install


LOGGER = logging.getLogger(__name__)
PRODUCER_SERVICE_NAME = "Spark->Kafka Producer"
GENERIC_HDFS_USER_PRINCIPAL = "client@{realm}".format(realm=sdk_auth.REALM)

DEFAULT_KAFKA_TASK_COUNT=3
KERBERIZED_KAFKA = True
KAFKA_KRB5="W2xpYmRlZmF1bHRzXQpkZWZhdWx0X3JlYWxtID0gTE9DQUwKCltyZW" \
           "FsbXNdCiAgTE9DQUwgPSB7CiAgICBrZGMgPSBrZGMubWFyYXRob24u" \
           "YXV0b2lwLmRjb3MudGhpc2Rjb3MuZGlyZWN0b3J5OjI1MDAKICB9Cg=="
# Currently using stub-universe so it's beta-kafka, for the last time
KAFKA_PACKAGE_NAME = "beta-kafka"
KAFKA_SERVICE_NAME = "secure-kafka" if KERBERIZED_KAFKA else "kafka"


def setup_module(module):
    utils.require_spark()
    utils.upload_file(os.environ["SCALA_TEST_JAR_PATH"])


def teardown_module(module):
    utils.teardown_spark()
    sdk_install.uninstall(KAFKA_PACKAGE_NAME, KAFKA_SERVICE_NAME)


@pytest.mark.runnow
@pytest.mark.sanity
def test_kafka():
    def producer_launched():
        return utils.streaming_job_launched(PRODUCER_SERVICE_NAME)

    def producer_started():
        return utils.streaming_job_running(PRODUCER_SERVICE_NAME)

    if utils.kafka_enabled():
        try:
            utils.require_kafka()
            kerberos_flag = "true" if utils.KERBERIZED_KAFKA else "false"  # flag for using kerberized kafka given to app
            stop_count = "48"  # some reasonable number
            broker_dns = utils.kafka_broker_dns()
            topic = "top1"

            big_file, big_file_url = "/mnt/mesos/sandbox/big.txt", "http://norvig.com/big.txt"

            # arguments to the application
            producer_args = " ".join([broker_dns, big_file, topic, kerberos_flag])

            uris = "spark.mesos.uris=http://norvig.com/big.txt"
            if utils.KERBERIZED_KAFKA:
                uris += ",http://s3-us-west-2.amazonaws.com/arand-sandbox-mesosphere/client-jaas-executor-grover.conf"

            common_args = [
                "--conf", "spark.mesos.containerizer=mesos",
                "--conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=2400s",
                "--conf", "spark.scheduler.minRegisteredResourcesRatio=1.0",
                "--conf", uris
            ]

            kerberos_args = [
                "--conf", "spark.mesos.driver.secret.names=__dcos_base64___keytab",
                "--conf", "spark.mesos.driver.secret.filenames=kafka-client.keytab",
                "--conf", "spark.mesos.executor.secret.names=__dcos_base64___keytab",
                "--conf", "spark.mesos.executor.secret.filenames=kafka-client.keytab",
                "--conf", "spark.mesos.task.labels=DCOS_SPACE:/spark",
                "--conf", "spark.executorEnv.KRB5_CONFIG_BASE64={}".format(utils.KAFKA_KRB5),
                "--conf", "spark.mesos.driverEnv.KRB5_CONFIG_BASE64={}".format(utils.KAFKA_KRB5),
                "--conf", "spark.driver.extraJavaOptions=-Djava.security.auth.login.config="
                          "/mnt/mesos/sandbox/client-jaas-executor-grover.conf",
                "--conf", "spark.executor.extraJavaOptions="
                          "-Djava.security.auth.login.config=/mnt/mesos/sandbox/client-jaas-executor-grover.conf",
                "--conf", "spark.mesos.uris="
                          "http://s3-us-west-2.amazonaws.com/arand-sandbox-mesosphere/client-jaas-executor-grover.conf,"
                          "http://norvig.com/big.txt"
            ]

            producer_config = ["--conf", "spark.cores.max=2", "--conf", "spark.executor.cores=2",
                               "--class", "KafkaProducer"] + common_args

            if utils.KERBERIZED_KAFKA:
                producer_config += kerberos_args

            producer_id = utils.submit_job(app_url=utils._scala_test_jar_url(),
                                           app_args=producer_args,
                                           app_name="/spark",
                                           args=producer_config)

            shakedown.wait_for(lambda: producer_launched(), ignore_exceptions=False, timeout_seconds=600)
            shakedown.wait_for(lambda: producer_started(), ignore_exceptions=False, timeout_seconds=600)

            consumer_config = ["--conf", "spark.cores.max=4", "--class", "KafkaConsumer"] + common_args

            if utils.KERBERIZED_KAFKA:
                consumer_config += kerberos_args

            consumer_args = " ".join([broker_dns, topic, stop_count, kerberos_flag])

            utils.run_tests(app_url=utils._scala_test_jar_url(),
                            app_args=consumer_args,
                            expected_output="Read {} words".format(stop_count),
                            app_name="/spark",
                            args=consumer_config)

            utils.kill_driver(producer_id, "/spark")
        finally:
            if shakedown.get_service(utils.KAFKA_SERVICE_NAME) is not None:
                shakedown.uninstall_package_and_wait(utils.KAFKA_PACKAGE_NAME, utils.KAFKA_SERVICE_NAME)

